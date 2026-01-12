#!/bin/bash

set -e

VM_NAME="ge-ml-training"
ZONE="us-east1-d"
MACHINE_TYPE="n1-highmem-16"
GPU_TYPE="nvidia-tesla-t4"
GPU_COUNT="1"
BOOT_DISK_SIZE="256GB"
DATA_DISK_NAME="${VM_NAME}-data"
DATA_DISK_SIZE="256GB"
DATA_DISK_TYPE="pd-ssd"
IMAGE_FAMILY="ubuntu-2404-lts-amd64"
IMAGE_PROJECT="ubuntu-os-cloud"
SNAPSHOT_POLICY_NAME="${VM_NAME}-daily-backup"
STATIC_IP_NAME="${VM_NAME}-ip"

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
Usage: $0 <command> [options]

Commands:
    create [OPTIONS]           Create the GPU VM with optional features
    destroy                    Destroy the GPU VM and associated resources
    status                     Check the status of the GPU VM

Options:
    --install-drivers          Include startup script to install NVIDIA drivers and CUDA toolkit
    --ssh-key <path>           Path to SSH public key file for direct SSH access (e.g., ~/.ssh/id_rsa.pub)
    --data-disk <disk-name>    Attach existing persistent disk instead of creating new one
    --zone <zone>              Override default zone (default: us-east1-d)
    -h, --help                 Show this help message

Examples:
    $0 create                                    Create VM with Ops Agent only
    $0 create --install-drivers                  Create VM with Ops Agent and NVIDIA drivers
    $0 create --ssh-key ~/.ssh/id_rsa.pub        Create VM with SSH key for direct access
    $0 create --data-disk my-training-data       Attach existing disk instead of creating new
    $0 create --zone us-east1-c                  Create VM in different zone
    $0 create --install-drivers --ssh-key ~/.ssh/id_rsa.pub   Create with all features
    $0 destroy --zone us-east1-c                 Delete VM (data disk preserved)
    $0 status                                    Show VM status

Features:
    - Static external IP address (preserved across VM recreations)
    - HTTP/HTTPS firewall rules enabled by default
    - Google Cloud Ops Agent installed for logs and metrics
    - Daily backups at 3am UTC (7 day retention)
    - 256GB SSD data disk attached (or use existing disk with --data-disk)
    - Data disks are preserved when VM is destroyed

Recommended zones for T4 GPU availability (US East Coast):
    - us-east1-d, us-east4-a, us-east4-b, us-central1-a
EOF
}

create_snapshot_policy() {
    echo "Creating snapshot policy: ${SNAPSHOT_POLICY_NAME}..."

    if gcloud compute resource-policies describe "${SNAPSHOT_POLICY_NAME}" --region="${ZONE%-*}" &>/dev/null; then
        echo "Snapshot policy already exists."
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
}

ensure_firewall_rules() {
    echo "Ensuring firewall rules exist for HTTP/HTTPS traffic..."

    if ! gcloud compute firewall-rules describe default-allow-http &>/dev/null; then
        echo "Creating firewall rule for HTTP traffic..."
        gcloud compute firewall-rules create default-allow-http \
            --allow=tcp:80 \
            --target-tags=http-server \
            --source-ranges=0.0.0.0/0 \
            --description="Allow HTTP traffic"
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
    else
        echo "HTTPS firewall rule already exists."
    fi

    echo "Firewall rules ready."
}

ensure_static_ip() {
    echo "Ensuring static IP address exists: ${STATIC_IP_NAME}..." >&2

    if gcloud compute addresses describe "${STATIC_IP_NAME}" --region="${ZONE%-*}" &>/dev/null; then
        echo "Static IP already exists." >&2
    else
        echo "Creating static IP address..." >&2
        gcloud compute addresses create "${STATIC_IP_NAME}" --region="${ZONE%-*}"
        echo "Static IP created successfully." >&2
    fi

    local static_ip=$(gcloud compute addresses describe "${STATIC_IP_NAME}" --region="${ZONE%-*}" --format="value(address)")
    echo "Static IP address: ${static_ip}" >&2
    echo "${static_ip}"
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
    local install_drivers="$1"
    local ssh_key_file="$2"
    local data_disk_name="$3"
    local ssh_username=""
    local ssh_key_content=""

    local use_existing_disk="false"
    local disk_name="${DATA_DISK_NAME}"

    if [[ -n "${data_disk_name}" ]]; then
        use_existing_disk="true"
        disk_name="${data_disk_name}"
        validate_existing_disk "${disk_name}"
    fi

    if [[ -n "${ssh_key_file}" ]]; then
        if [[ ! -f "${ssh_key_file}" ]]; then
            echo "Error: SSH key file not found: ${ssh_key_file}"
            exit 1
        fi

        ssh_key_content=$(cat "${ssh_key_file}")

        ssh_username=$(echo "${ssh_key_content}" | awk '{print $3}' | cut -d'@' -f1)
        if [[ -z "${ssh_username}" ]]; then
            ssh_username="${USER}"
        fi

        echo "SSH key will be added for user: ${ssh_username}"
    fi

    echo "Creating GPU VM: ${VM_NAME}..."
    echo "  Machine Type: ${MACHINE_TYPE}"
    echo "  GPU: ${GPU_COUNT}x ${GPU_TYPE}"
    echo "  Zone: ${ZONE}"
    echo "  Boot Disk: ${BOOT_DISK_SIZE}"
    if [[ "${use_existing_disk}" == "true" ]]; then
        echo "  Data Disk: ${disk_name} (existing)"
    else
        echo "  Data Disk: ${disk_name} (new ${DATA_DISK_SIZE} ${DATA_DISK_TYPE})"
    fi
    echo "  Install Drivers: ${install_drivers}"
    echo "  SSH Key: ${ssh_key_file:-none}"
    echo "  Ops Agent: enabled"
    echo "  HTTP/HTTPS: enabled"
    echo "  Static IP: enabled"
    echo ""

    create_snapshot_policy
    ensure_firewall_rules
    local static_ip=$(ensure_static_ip)
    echo ""

    local temp_script=$(mktemp)
    echo "#!/bin/bash" > "${temp_script}"
    echo "" >> "${temp_script}"
    echo "${OPS_AGENT_SCRIPT}" >> "${temp_script}"
    echo "" >> "${temp_script}"
    echo "${DATA_DISK_SETUP_SCRIPT}" >> "${temp_script}"

    if [[ "${install_drivers}" == "true" ]]; then
        echo "" >> "${temp_script}"
        echo "${NVIDIA_DRIVER_SCRIPT}" >> "${temp_script}"
    fi

    local disk_arg=""
    if [[ "${use_existing_disk}" == "true" ]]; then
        disk_arg="--disk=name=${disk_name},mode=rw,boot=no,auto-delete=no"
    else
        disk_arg="--create-disk=name=${disk_name},size=${DATA_DISK_SIZE},type=${DATA_DISK_TYPE},mode=rw,auto-delete=no"
    fi

    local create_cmd=(
        gcloud compute instances create "${VM_NAME}"
        --zone="${ZONE}"
        --machine-type="${MACHINE_TYPE}"
        --accelerator="type=${GPU_TYPE},count=${GPU_COUNT}"
        --maintenance-policy=TERMINATE
        --boot-disk-size="${BOOT_DISK_SIZE}"
        --image-family="${IMAGE_FAMILY}"
        --image-project="${IMAGE_PROJECT}"
        ${disk_arg}
        --tags=http-server,https-server
        --metadata-from-file=startup-script="${temp_script}"
        --address="${static_ip}"
    )

    if [[ -n "${ssh_key_file}" ]]; then
        create_cmd+=(--metadata=ssh-keys="${ssh_username}:${ssh_key_content}")
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

        echo "Attaching snapshot policy to data disk..."
        if gcloud compute disks add-resource-policies "${disk_name}" \
            --resource-policies="${SNAPSHOT_POLICY_NAME}" \
            --zone="${ZONE}"; then
            echo "Data disk snapshot policy attached successfully."
        else
            echo "Warning: Failed to attach snapshot policy to data disk."
        fi
        echo ""

        echo "Static IP Address: ${static_ip} (reserved)"
        echo ""

        if [[ -n "${ssh_key_file}" ]]; then
            echo "SSH Access (direct):"
            echo "  ssh ${ssh_username}@${static_ip}"
            echo ""
        fi

        echo "SSH Access (via gcloud):"
        echo "  gcloud compute ssh ${VM_NAME} --zone=${ZONE}"
        echo ""

        echo "Startup script is running and will:"
        echo "  - Install Google Cloud Ops Agent for logs and metrics"
        echo "  - Format and mount data disk at /mnt/data"
        if [[ "${install_drivers}" == "true" ]]; then
            echo "  - Install NVIDIA drivers and CUDA toolkit"
        fi
        echo ""

        if [[ "${install_drivers}" == "true" ]]; then
            echo "NVIDIA driver installation may take several minutes."
            echo "Check installation status:"
            echo "  gcloud compute ssh ${VM_NAME} --zone=${ZONE} --command='tail -f /var/log/syslog | grep cuda'"
            echo ""
            echo "Verify driver installation:"
            echo "  gcloud compute ssh ${VM_NAME} --zone=${ZONE} --command='nvidia-smi'"
            echo ""
        fi

        echo "Data disk will be automatically formatted and mounted at /mnt/data"
        echo "HTTP/HTTPS traffic is allowed via firewall rules."

        rm -f "${temp_script}"
    else
        echo ""
        echo "Failed to create VM."
        echo ""
        echo "If you encountered a ZONE_RESOURCE_POOL_EXHAUSTED error, try a different zone:"
        echo "  ./training/manage-gpu-vm.sh create --zone us-east1-d"
        echo "  ./training/manage-gpu-vm.sh create --zone us-east4-a"
        echo "  ./training/manage-gpu-vm.sh create --zone us-central1-a"
        echo ""
        rm -f "${temp_script}"
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

    local command="$1"
    shift

    case "${command}" in
        create)
            local install_drivers="false"
            local ssh_key_file=""
            local data_disk_name=""
            local zone_override=""
            while [[ $# -gt 0 ]]; do
                case "$1" in
                    --install-drivers)
                        install_drivers="true"
                        shift
                        ;;
                    --ssh-key)
                        if [[ -z "$2" || "$2" == --* ]]; then
                            echo "Error: --ssh-key requires a file path argument"
                            exit 1
                        fi
                        ssh_key_file="$2"
                        shift 2
                        ;;
                    --data-disk)
                        if [[ -z "$2" || "$2" == --* ]]; then
                            echo "Error: --data-disk requires a disk name argument"
                            exit 1
                        fi
                        data_disk_name="$2"
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

            create_vm "${install_drivers}" "${ssh_key_file}" "${data_disk_name}"
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
