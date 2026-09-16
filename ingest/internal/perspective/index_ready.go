package perspective

import (
	"fmt"
	"sort"
)

// RequiredIndexFields are the mappings a destination index must already have
// before this service writes Perspective fields into it, as field name ->
// Elasticsearch type.
//
// perspective_scores is represented by one representative attribute rather
// than all fifteen: the template maps the object with an explicit property
// list and dynamic:false, so if one attribute is mapped the object came from
// the template and all of them are.
var RequiredIndexFields = map[string]string{
	"combined_perspective_score":  "float",
	"perspective_scored_at":       "date",
	"perspective_scores.toxicity": "float",
}

// IndexMappingReady reports whether an index whose mapped field types are
// types can safely receive Perspective fields, and if not, why.
//
// An index template applies only when an index is created, so an index that
// already existed when the template gained these fields does not have them.
// Writing anyway would let dynamic mapping infer the types, and the inference
// is driven by whichever document happens to arrive first: Go renders
// float64(0) as the JSON integer 0, so a maximally toxic post arriving first
// makes Elasticsearch map combined_perspective_score as a long. Every
// fractional score after that is silently truncated to 0 in the index —
// accepted, not rejected — and a field's type cannot be changed in place
// afterwards. _source keeps the true value, so serving still reads the right
// number, but the field is mapped index:true precisely so it can be queried,
// filtered and aggregated, and that would quietly return nonsense.
//
// So this is a gate, not a warning: leave the posts unscored, let the api
// score them live as it already does for anything unscored, and let
// cmd/backfill_perspective fill them in once the mapping is right. The next
// period index is created from the current template, so a deploy that lands
// mid-period starts scoring on its own at the boundary with no intervention.
func IndexMappingReady(types map[string]string) (bool, string) {
	var problems []string
	for field, want := range RequiredIndexFields {
		got, ok := types[field]
		switch {
		case !ok:
			problems = append(problems, fmt.Sprintf("%s is not mapped", field))
		case got != want:
			problems = append(problems, fmt.Sprintf("%s is mapped as %s, want %s", field, got, want))
		}
	}
	if len(problems) == 0 {
		return true, ""
	}
	sort.Strings(problems)
	reason := problems[0]
	for _, p := range problems[1:] {
		reason += "; " + p
	}
	return false, reason
}
