#!/bin/bash

set -e

# VM_NAME will be set from positional argument
ZONE="us-east1-d"
MACHINE_TYPE="n1-highmem-16"
GPU_TYPE="nvidia-tesla-t4"
GPU_COUNT="1"
BOOT_DISK_SIZE="256GB"
DATA_DISK_SIZE="256GB"
DATA_DISK_TYPE="pd-ssd"
IMAGE_FAMILY="ubuntu-2404-lts-amd64"
IMAGE_PROJECT="ubuntu-os-cloud"
SNAPSHOT_POLICY_NAME="ge-ml-training-daily-backup"
# STATIC_IP_NAME will be derived from VM_NAME

OPS_AGENT_SCRIPT='#!/bin/bash
curl -sSO https://dl.google.com/cloudagents/add-google-cloud-ops-agent-repo.sh
sudo bash add-google-cloud-ops-agent-repo.sh --also-install
'

DATA_DISK_SETUP_SCRIPT='
# Setup data disk (idempotent - checks disk itself, not fstab)
DISK_DEVICE="/dev/disk/by-id/google-persistent-disk-1"

# Check if disk already has a filesystem
if blkid "${DISK_DEVICE}" &>/dev/null; then
  echo "Data disk already has a filesystem, skipping format"
else
  echo "Formatting new data disk..."
  mkfs.ext4 "${DISK_DEVICE}"
fi

# Mount if not already mounted
if ! mountpoint -q /mnt/data; then
  mkdir -p /mnt/data
  mount "${DISK_DEVICE}" /mnt/data
  echo "Data disk mounted at /mnt/data"
fi

# Add to fstab if not already there
if ! grep -q "/mnt/data" /etc/fstab; then
  echo "${DISK_DEVICE} /mnt/data ext4 defaults 0 2" >> /etc/fstab
  echo "Added to /etc/fstab"
fi
'

NVIDIA_DRIVER_SCRIPT='
if test -f /opt/google/cuda-installer; then
  exit 0
fi

mkdir -p /opt/google/cuda-installer
cd /opt/google/cuda-installer/ || exit 1

curl -fSsL -O https://storage.googleapis.com/compute-gpu-installation-us/installer/latest/cuda_installer.pyz
python3 cuda_installer.pyz install_cuda
'

usage() {
    cat << EOF
Usage: $0 <vm-name> <command> [options]

Commands:
    create [OPTIONS]           Create the GPU VM with optional features
    destroy                    Destroy the GPU VM and associated resources
    status                     Check the status of the GPU VM

Options:
    --attach-disk <disk-name>  Attach existing persistent disk instead of creating new one
    --create-disk <disk-name>  Create new persistent disk with specified name
    --zone <zone>              Override default zone (default: us-east1-d)
    -h, --help                 Show this help message

Examples:
    $0 my-vm create                                    Create VM with boot disk only
    $0 my-vm create --create-disk my-data              Create VM with new named data disk
    $0 my-vm create --attach-disk existing-data        Attach existing disk instead of creating new
    $0 my-vm create --zone us-east1-c                  Create VM in different zone
    $0 my-vm destroy --zone us-east1-c                 Delete VM (data disk preserved)
    $0 my-vm status                                    Show VM status

Features:
    - Static external IP address (preserved across VM recreations)
    - HTTP/HTTPS firewall rules enabled by default
    - Google Cloud Ops Agent installed for logs and metrics
    - NVIDIA drivers and CUDA toolkit installed automatically
    - Daily backups at 3am UTC (7 day retention, policy: ge-ml-training-daily-backup)
    - Optional 256GB SSD data disk (use --create-disk or --attach-disk)
    - Data disks are preserved when VM is destroyed
    - SSH keys must be added manually via GCP Console

Recommended zones for T4 GPU availability (US East Coast):
    - us-east1-d, us-east4-a, us-east4-b, us-central1-a
EOF
}

validate_vm_name() {
    local vm_name="$1"

    # GCP resource naming requirements:
    # - Must start with a lowercase letter
    # - Can contain lowercase letters, numbers, and hyphens
    # - Must end with a lowercase letter or number
    # - Length: 1-63 characters

    if [[ ! "${vm_name}" =~ ^[a-z]([a-z0-9-]{0,61}[a-z0-9])?$ ]]; then
        echo "Error: Invalid VM name '${vm_name}'"
        echo ""
        echo "VM name must:"
        echo "  - Start with a lowercase letter"
        echo "  - Contain only lowercase letters, numbers, and hyphens"
        echo "  - End with a lowercase letter or number"
        echo "  - Be 1-63 characters long"
        echo ""
        echo "Examples of valid names:"
        echo "  - my-vm"
        echo "  - ge-ml-training"
        echo "  - vm-2026-01-12-212802"
        exit 1
    fi
}

create_snapshot_policy() {
    echo "Creating snapshot policy: ${SNAPSHOT_POLICY_NAME}..."

    if gcloud compute resource-policies describe "${SNAPSHOT_POLICY_NAME}" --region="${ZONE%-*}" &>/dev/null; then
        echo "Snapshot policy already exists."
        CREATED_SNAPSHOT_POLICY=false
        return 0
    fi

    gcloud compute resource-policies create snapshot-schedule "${SNAPSHOT_POLICY_NAME}" \
        --region="${ZONE%-*}" \
        --max-retention-days=7 \
        --on-source-disk-delete=keep-auto-snapshots \
        --daily-schedule \
        --start-time=03:00 \
        --storage-location=us

    echo "Snapshot policy created successfully."
    CREATED_SNAPSHOT_POLICY=true
}

ensure_firewall_rules() {
    echo "Ensuring firewall rules exist for HTTP/HTTPS traffic..."

    CREATED_HTTP_FIREWALL=false
    CREATED_HTTPS_FIREWALL=false

    if ! gcloud compute firewall-rules describe default-allow-http &>/dev/null; then
        echo "Creating firewall rule for HTTP traffic..."
        gcloud compute firewall-rules create default-allow-http \
            --allow=tcp:80 \
            --target-tags=http-server \
            --source-ranges=0.0.0.0/0 \
            --description="Allow HTTP traffic"
        CREATED_HTTP_FIREWALL=true
    else
        echo "HTTP firewall rule already exists."
    fi

    if ! gcloud compute firewall-rules describe default-allow-https &>/dev/null; then
        echo "Creating firewall rule for HTTPS traffic..."
        gcloud compute firewall-rules create default-allow-https \
            --allow=tcp:443 \
            --target-tags=https-server \
            --source-ranges=0.0.0.0/0 \
            --description="Allow HTTPS traffic"
        CREATED_HTTPS_FIREWALL=true
    else
        echo "HTTPS firewall rule already exists."
    fi

    echo "Firewall rules ready."
}

ensure_static_ip() {
    echo "Ensuring static IP address exists: ${STATIC_IP_NAME}..."

    if gcloud compute addresses describe "${STATIC_IP_NAME}" --region="${ZONE%-*}" &>/dev/null; then
        echo "Static IP already exists."
        CREATED_STATIC_IP=false
    else
        echo "Creating static IP address..."
        gcloud compute addresses create "${STATIC_IP_NAME}" --region="${ZONE%-*}"
        echo "Static IP created successfully."
        CREATED_STATIC_IP=true
    fi

    STATIC_IP_ADDRESS=$(gcloud compute addresses describe "${STATIC_IP_NAME}" --region="${ZONE%-*}" --format="value(address)")
    echo "Static IP address: ${STATIC_IP_ADDRESS}"
}

cleanup_on_failure() {
    echo ""
    echo "Cleaning up newly created resources due to VM creation failure..."

    # Check if a data disk was potentially created during failed VM creation
    if [[ "${CREATING_DATA_DISK}" == "true" ]] && [[ -n "${DATA_DISK_TO_CLEANUP}" ]]; then
        if gcloud compute disks describe "${DATA_DISK_TO_CLEANUP}" --zone="${ZONE}" &>/dev/null; then
            echo "Deleting data disk: ${DATA_DISK_TO_CLEANUP}..."
            if gcloud compute disks delete "${DATA_DISK_TO_CLEANUP}" --zone="${ZONE}" --quiet 2>/dev/null; then
                echo "  Data disk deleted."
            else
                echo "  Warning: Failed to delete data disk."
            fi
        fi
    fi

    if [[ "${CREATED_STATIC_IP}" == "true" ]]; then
        echo "Deleting static IP: ${STATIC_IP_NAME}..."
        if gcloud compute addresses delete "${STATIC_IP_NAME}" --region="${ZONE%-*}" --quiet 2>/dev/null; then
            echo "  Static IP deleted."
        else
            echo "  Warning: Failed to delete static IP."
        fi
    fi

    if [[ "${CREATED_SNAPSHOT_POLICY}" == "true" ]]; then
        echo "Deleting snapshot policy: ${SNAPSHOT_POLICY_NAME}..."
        if gcloud compute resource-policies delete "${SNAPSHOT_POLICY_NAME}" --region="${ZONE%-*}" --quiet 2>/dev/null; then
            echo "  Snapshot policy deleted."
        else
            echo "  Warning: Failed to delete snapshot policy."
        fi
    fi

    if [[ "${CREATED_HTTP_FIREWALL}" == "true" ]]; then
        echo "Deleting HTTP firewall rule..."
        if gcloud compute firewall-rules delete default-allow-http --quiet 2>/dev/null; then
            echo "  HTTP firewall rule deleted."
        else
            echo "  Warning: Failed to delete HTTP firewall rule."
        fi
    fi

    if [[ "${CREATED_HTTPS_FIREWALL}" == "true" ]]; then
        echo "Deleting HTTPS firewall rule..."
        if gcloud compute firewall-rules delete default-allow-https --quiet 2>/dev/null; then
            echo "  HTTPS firewall rule deleted."
        else
            echo "  Warning: Failed to delete HTTPS firewall rule."
        fi
    fi

    echo "Cleanup complete."
}

validate_existing_disk() {
    local disk_name="$1"

    # Check if disk exists
    if ! gcloud compute disks describe "${disk_name}" --zone="${ZONE}" &>/dev/null; then
        echo "Error: Disk '${disk_name}' not found in zone ${ZONE}"
        exit 1
    fi

    # Verify disk is not already attached
    local attached_instances=$(gcloud compute disks describe "${disk_name}" \
        --zone="${ZONE}" \
        --format="value(users)")

    if [[ -n "${attached_instances}" ]]; then
        echo "Error: Disk '${disk_name}' is already attached to: ${attached_instances}"
        exit 1
    fi

    echo "Validated existing disk: ${disk_name}"
}

create_vm() {
    local disk_operation="$1"  # "attach", "create", or empty
    local disk_name="$2"       # disk name if operation specified

    # Initialize resource tracking variables
    CREATED_SNAPSHOT_POLICY=false
    CREATED_HTTP_FIREWALL=false
    CREATED_HTTPS_FIREWALL=false
    CREATED_STATIC_IP=false
    CREATING_DATA_DISK=false
    DATA_DISK_TO_CLEANUP=""

    local use_existing_disk="none"

    if [[ "${disk_operation}" == "attach" ]]; then
        use_existing_disk="true"
        validate_existing_disk "${disk_name}"
    elif [[ "${disk_operation}" == "create" ]]; then
        use_existing_disk="false"
        CREATING_DATA_DISK=true
        DATA_DISK_TO_CLEANUP="${disk_name}"
    fi

    echo "Creating GPU VM: ${VM_NAME}..."
    echo "  Machine Type: ${MACHINE_TYPE}"
    echo "  GPU: ${GPU_COUNT}x ${GPU_TYPE}"
    echo "  Zone: ${ZONE}"
    echo "  Boot Disk: ${BOOT_DISK_SIZE}"
    if [[ "${use_existing_disk}" == "true" ]]; then
        echo "  Data Disk: ${disk_name} (attaching existing)"
    elif [[ "${use_existing_disk}" == "false" ]]; then
        echo "  Data Disk: ${disk_name} (creating new ${DATA_DISK_SIZE} ${DATA_DISK_TYPE})"
    else
        echo "  Data Disk: none (boot disk only)"
    fi
    echo "  NVIDIA Drivers: will be installed"
    echo "  Ops Agent: enabled"
    echo "  HTTP/HTTPS: enabled"
    echo "  Static IP: enabled"
    echo ""

    create_snapshot_policy
    ensure_firewall_rules
    ensure_static_ip
    echo ""

    local temp_script=$(mktemp)
    echo "#!/bin/bash" > "${temp_script}"
    echo "" >> "${temp_script}"
    echo "${OPS_AGENT_SCRIPT}" >> "${temp_script}"

    # Only add disk setup if we're creating or attaching a persistent disk
    if [[ "${use_existing_disk}" != "none" ]]; then
        echo "" >> "${temp_script}"
        echo "${DATA_DISK_SETUP_SCRIPT}" >> "${temp_script}"
    fi

    # Always install NVIDIA drivers
    echo "" >> "${temp_script}"
    echo "${NVIDIA_DRIVER_SCRIPT}" >> "${temp_script}"

    local disk_arg=""
    if [[ "${use_existing_disk}" == "true" ]]; then
        disk_arg="--disk=name=${disk_name},mode=rw,boot=no,auto-delete=no"
    elif [[ "${use_existing_disk}" == "false" ]]; then
        disk_arg="--create-disk=name=${disk_name},size=${DATA_DISK_SIZE},type=${DATA_DISK_TYPE},mode=rw,auto-delete=no"
    fi
    # If use_existing_disk is "none", disk_arg remains empty

    local create_cmd=(
        gcloud compute instances create "${VM_NAME}"
        --zone="${ZONE}"
        --machine-type="${MACHINE_TYPE}"
        --accelerator="type=${GPU_TYPE},count=${GPU_COUNT}"
        --maintenance-policy=TERMINATE
        --boot-disk-size="${BOOT_DISK_SIZE}"
        --image-family="${IMAGE_FAMILY}"
        --image-project="${IMAGE_PROJECT}"
        --tags=http-server,https-server
        --metadata-from-file=startup-script="${temp_script}"
        --address="${STATIC_IP_ADDRESS}"
    )

    # Add disk argument if present
    if [[ -n "${disk_arg}" ]]; then
        create_cmd+=(${disk_arg})
    fi

    if "${create_cmd[@]}"; then
        echo ""
        echo "VM created successfully!"
        echo ""

        echo "Attaching snapshot policy to boot disk..."
        if gcloud compute disks add-resource-policies "${VM_NAME}" \
            --resource-policies="${SNAPSHOT_POLICY_NAME}" \
            --zone="${ZONE}"; then
            echo "Boot disk snapshot policy attached successfully."
        else
            echo "Warning: Failed to attach snapshot policy to boot disk."
        fi

        # Only attach to data disk if one was created or attached
        if [[ "${use_existing_disk}" != "none" ]]; then
            echo "Attaching snapshot policy to data disk..."
            if gcloud compute disks add-resource-policies "${disk_name}" \
                --resource-policies="${SNAPSHOT_POLICY_NAME}" \
                --zone="${ZONE}"; then
                echo "Data disk snapshot policy attached successfully."
            else
                echo "Warning: Failed to attach snapshot policy to data disk."
            fi
        fi
        echo ""

        echo "Static IP Address: ${STATIC_IP_ADDRESS} (reserved)"
        echo ""

        echo "SSH Access (via gcloud):"
        echo "  gcloud compute ssh ${VM_NAME} --zone=${ZONE}"
        echo ""
        echo "Note: For direct SSH access, add SSH keys manually via GCP Console"
        echo ""

        echo "Startup script is running and will:"
        echo "  - Install Google Cloud Ops Agent for logs and metrics"
        if [[ "${use_existing_disk}" != "none" ]]; then
            echo "  - Format and mount data disk at /mnt/data"
        fi
        echo "  - Install NVIDIA drivers and CUDA toolkit"
        echo ""

        echo "NVIDIA driver installation may take several minutes."
        echo "Check installation status:"
        echo "  gcloud compute ssh ${VM_NAME} --zone=${ZONE} --command='tail -f /var/log/syslog | grep cuda'"
        echo ""
        echo "Verify driver installation:"
        echo "  gcloud compute ssh ${VM_NAME} --zone=${ZONE} --command='nvidia-smi'"
        echo ""

        if [[ "${use_existing_disk}" != "none" ]]; then
            echo "Data disk will be automatically formatted and mounted at /mnt/data"
        fi
        echo "HTTP/HTTPS traffic is allowed via firewall rules."

        rm -f "${temp_script}"
    else
        echo ""
        echo "Failed to create VM."
        rm -f "${temp_script}"

        # Clean up any resources that were created during this attempt
        cleanup_on_failure

        echo ""
        echo "If you encountered a ZONE_RESOURCE_POOL_EXHAUSTED error, try a different zone:"
        echo "  ./training/manage-gpu-vm.sh ${VM_NAME} create --zone us-east1-d"
        echo "  ./training/manage-gpu-vm.sh ${VM_NAME} create --zone us-east4-a"
        echo "  ./training/manage-gpu-vm.sh ${VM_NAME} create --zone us-central1-a"
        echo ""
        exit 1
    fi
}

destroy_vm() {
    echo "This will destroy the following resources in zone ${ZONE}:"
    echo "  - VM: ${VM_NAME}"
    echo ""
    echo "The following resources will be PRESERVED:"
    echo "  - Data Disk (will be detached but not deleted)"
    echo "  - Snapshot Policy: ${SNAPSHOT_POLICY_NAME}"
    echo "  - Static IP: ${STATIC_IP_NAME}"
    echo ""
    read -p "Are you sure you want to continue? (yes/no): " confirmation

    if [[ "${confirmation}" != "yes" ]]; then
        echo "Aborted."
        exit 0
    fi

    echo "Deleting VM: ${VM_NAME}..."
    if gcloud compute instances describe "${VM_NAME}" --zone="${ZONE}" &>/dev/null; then
        gcloud compute instances delete "${VM_NAME}" --zone="${ZONE}" --quiet
        echo "VM deleted."
    else
        echo "VM does not exist in zone ${ZONE}."
    fi

    echo "Detaching snapshot policies from disks..."
    if gcloud compute disks describe "${VM_NAME}" --zone="${ZONE}" &>/dev/null 2>&1; then
        gcloud compute disks remove-resource-policies "${VM_NAME}" \
            --resource-policies="${SNAPSHOT_POLICY_NAME}" \
            --zone="${ZONE}" &>/dev/null || echo "Boot disk policy already detached or disk deleted."
    fi

    local attached_disks=$(gcloud compute instances describe "${VM_NAME}" \
        --zone="${ZONE}" \
        --format="value(disks[].source.basename())" 2>/dev/null || echo "")

    for disk in ${attached_disks}; do
        if [[ "${disk}" != "${VM_NAME}" ]]; then
            echo "Detaching snapshot policy from data disk: ${disk}..."
            gcloud compute disks remove-resource-policies "${disk}" \
                --resource-policies="${SNAPSHOT_POLICY_NAME}" \
                --zone="${ZONE}" &>/dev/null || echo "  Policy already detached."
            echo "Data disk '${disk}' preserved (not deleted)."
        fi
    done

    echo "VM destroyed successfully. Data disk(s) preserved."
}

show_status() {
    echo "Checking status of VM: ${VM_NAME}..."
    echo ""

    if gcloud compute instances describe "${VM_NAME}" --zone="${ZONE}" --format="table(name,status,machineType.basename(),networkInterfaces[0].accessConfigs[0].natIP:label=EXTERNAL_IP)" 2>/dev/null; then
        echo ""
        echo "GPU Information:"
        gcloud compute instances describe "${VM_NAME}" --zone="${ZONE}" --format="value(guestAccelerators[0].acceleratorType.basename(),guestAccelerators[0].acceleratorCount)" 2>/dev/null | while read -r gpu_info; do
            echo "  ${gpu_info}"
        done
        echo ""
        echo "Disks:"
        gcloud compute instances describe "${VM_NAME}" --zone="${ZONE}" --format="table(disks[].source.basename(),disks[].diskSizeGb)" 2>/dev/null
    else
        echo "VM does not exist."
        exit 1
    fi
}

main() {
    if [[ $# -eq 0 ]]; then
        usage
        exit 1
    fi

    # First argument is VM name
    VM_NAME="$1"
    shift

    # Validate VM name meets GCP requirements
    validate_vm_name "${VM_NAME}"

    if [[ $# -eq 0 ]]; then
        echo "Error: Command required after VM name"
        usage
        exit 1
    fi

    # Derive STATIC_IP_NAME from VM name
    STATIC_IP_NAME="${VM_NAME}-ip"

    local command="$1"
    shift

    case "${command}" in
        create)
            local disk_operation=""
            local disk_name=""
            local zone_override=""
            while [[ $# -gt 0 ]]; do
                case "$1" in
                    --attach-disk)
                        if [[ -z "$2" || "$2" == --* ]]; then
                            echo "Error: --attach-disk requires a disk name argument"
                            exit 1
                        fi
                        if [[ -n "${disk_operation}" ]]; then
                            echo "Error: Cannot use both --attach-disk and --create-disk"
                            exit 1
                        fi
                        disk_operation="attach"
                        disk_name="$2"
                        shift 2
                        ;;
                    --create-disk)
                        if [[ -z "$2" || "$2" == --* ]]; then
                            echo "Error: --create-disk requires a disk name argument"
                            exit 1
                        fi
                        if [[ -n "${disk_operation}" ]]; then
                            echo "Error: Cannot use both --attach-disk and --create-disk"
                            exit 1
                        fi
                        disk_operation="create"
                        disk_name="$2"
                        shift 2
                        ;;
                    --zone)
                        if [[ -z "$2" || "$2" == --* ]]; then
                            echo "Error: --zone requires a zone argument"
                            exit 1
                        fi
                        zone_override="$2"
                        shift 2
                        ;;
                    -h|--help)
                        usage
                        exit 0
                        ;;
                    *)
                        echo "Unknown option: $1"
                        usage
                        exit 1
                        ;;
                esac
            done

            if [[ -n "${zone_override}" ]]; then
                ZONE="${zone_override}"
            fi

            create_vm "${disk_operation}" "${disk_name}"
            ;;
        destroy)
            local zone_override=""
            while [[ $# -gt 0 ]]; do
                case "$1" in
                    --zone)
                        if [[ -z "$2" || "$2" == --* ]]; then
                            echo "Error: --zone requires a zone argument"
                            exit 1
                        fi
                        zone_override="$2"
                        shift 2
                        ;;
                    -h|--help)
                        usage
                        exit 0
                        ;;
                    *)
                        echo "Unknown option: $1"
                        usage
                        exit 1
                        ;;
                esac
            done

            if [[ -n "${zone_override}" ]]; then
                ZONE="${zone_override}"
            fi

            destroy_vm
            ;;
        status)
            show_status
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown command: ${command}"
            usage
            exit 1
            ;;
    esac
}

main "$@"
