//! `Query` methods serving the eslselect hits/stats-query endpoints — port of
//! `AddCountByTimePipe`, `GetStatsLabels`, `GetStatsLabelsAddGroupingByTime`,
//! `GetFixedFields` and `IsFixedOutputFieldsOrder` from `parser.go`.
//!
//! PORT NOTE: Go downcasts pipes (`*pipeStats`, `*pipeSort`, the big
//! type-switch over the pipes after the last `stats` pipe, ...); the port
//! dispatches through the object-safe hooks on [`crate::pipe::Pipe`]
//! (`stats_pipe_fields`, `stats_add_by_time_field`, `add_partition_by_time`,
//! `stats_labels_tail_op`, `fixed_fields_transparent`, `fixed_result_fields`,
//! `sort_adjust_result_fields_order`, `subquery_is_fixed_output_fields_order`,
//! `is_safe_for_hits`).

use std::collections::HashSet;

use crate::parser::quote_token_if_needed;
use crate::pipe::{Pipe, StatsTailOp};
use crate::prefix_filter;
use crate::values_encoder::marshal_duration_string;

use super::query::Query;

impl Query {
    /// Adds `| stats by (_time:step offset off, field1, ..., fieldN) count() hits`
    /// to the end of q (Go `AddCountByTimePipe`).
    pub fn add_count_by_time_pipe(&mut self, step: i64, off: i64, fields: &[String]) {
        // Drop pipes from q, which modify or delete _time field, since they
        // make impossible to calculate stats grouped by _time.
        self.drop_pipes_unsafe_for_hits();

        {
            // add 'stats by (_time:step offset off, fields) count() hits'
            let step_str = duration_string(step);
            let mut by_fields_str = format!("_time:{step_str}");
            if off != 0 {
                let offset_str = duration_string(off);
                by_fields_str += " offset ";
                by_fields_str += &offset_str;
            }
            for f in fields {
                by_fields_str += ", ";
                by_fields_str += &quote_token_if_needed(f);
            }
            let hits_field_name = get_unique_result_name("hits", fields);
            let s = format!(
                "stats by ({by_fields_str}) count() {}",
                quote_token_if_needed(&hits_field_name)
            );

            self.must_append_pipe(&s);
        }

        {
            // Add 'sort by (_time, fields)' in order to get consistent order
            // of the results.
            let mut sort_fields_str = "_time".to_string();
            for f in fields {
                sort_fields_str += ", ";
                sort_fields_str += &quote_token_if_needed(f);
            }
            let s = format!("sort by ({sort_fields_str})");

            self.must_append_pipe(&s);
        }
    }

    /// Drops trailing pipes from q, which are unsafe for calculating hits
    /// grouped by `_time` (Go `dropPipesUnsafeForHits`).
    pub(crate) fn drop_pipes_unsafe_for_hits(&mut self) {
        let timestamp = self.get_timestamp();
        for i in 0..self.pipes.len() {
            if !self.pipes[i].is_safe_for_hits(timestamp) {
                // Drop the rest of the pipes, including the current pipe,
                // since it modifies or deletes the _time field.
                self.pipes.truncate(i);
                return;
            }
        }
    }

    /// Returns stats labels from q for the `/select/logsql/stats_query`
    /// endpoint (Go `GetStatsLabels`). The remaining fields are considered
    /// metrics.
    pub fn get_stats_labels(&mut self) -> Result<Vec<String>, String> {
        self.get_stats_labels_add_grouping_by_time(0, 0)
    }

    /// Returns stats labels from q for the `/select/logsql/stats_query` and
    /// `/select/logsql/stats_query_range` endpoints
    /// (Go `GetStatsLabelsAddGroupingByTime`).
    ///
    /// If `step > 0`, then `_time:step` is added to the last
    /// `stats by (...)` pipe at q.
    pub fn get_stats_labels_add_grouping_by_time(
        &mut self,
        step: i64,
        offset: i64,
    ) -> Result<Vec<String>, String> {
        let Some(idx) = get_last_pipe_stats_idx(&self.pipes) else {
            return Err(format!("missing `| stats ...` pipe in the query [{self}]"));
        };

        // For range stats (step > 0), verify that pipes in front of the last
        // `stats` pipe do not modify or delete the `_time` field, since it is
        // required for bucketing by step. For instant stats (step == 0), allow
        // such pipes for broader query flexibility.
        if step > 0 {
            for i in 0..idx {
                let p = &self.pipes[i];
                if p.stats_pipe_fields().is_some() {
                    // Skip `stats` pipe, since it is updated with the grouping
                    // by `_time` in the add_by_time_field_to_stats_pipes() below.
                    continue;
                }
                if !p.can_return_last_n_results() {
                    return Err(format!(
                        "the pipe `| {:?}` cannot be put in front of `| {:?}`, since it may modify or delete `_time` field",
                        p.to_string(),
                        self.pipes[idx].to_string()
                    ));
                }
            }
        }

        // add _time:step to by (...) list at stats pipes.
        self.add_by_time_field_to_stats_pipes(step, offset);

        // PORT NOTE: Go calls initStatsRateFuncStepsNoSubqueries() here; rate
        // func step initialization is deferred crate-wide (see
        // `query.rs::init_stats_rate_func_steps`). It affects only the
        // execution of `rate()`/`rate_sum()`, not the returned labels or the
        // query string.

        // add 'partition by (_time)' to 'sort', 'first' and 'last' pipes.
        self.add_partition_by_time(step);

        let ps = self.pipes[idx]
            .stats_pipe_fields()
            .expect("BUG: pipes[idx] must be a stats pipe");
        let ps_str = self.pipes[idx].to_string();

        let mut label_fields: Vec<String> = Vec::with_capacity(ps.by_fields.len());
        let mut metric_fields: HashSet<String> = HashSet::with_capacity(ps.funcs.len());

        // extract by(...) field names from ps
        for f in &ps.by_fields {
            add_to_label_fields(&mut label_fields, &mut metric_fields, f);
        }

        // extract metric fields from stats pipe
        for (result_name, is_row_label) in &ps.funcs {
            if *is_row_label {
                add_to_label_fields(&mut label_fields, &mut metric_fields, result_name);
            } else {
                add_to_metric_fields(&mut label_fields, &mut metric_fields, result_name);
            }
        }

        // verify that all the pipes after the idx do not add new fields
        for i in idx + 1..self.pipes.len() {
            let p = &self.pipes[i];
            let Some(op) = p.stats_labels_tail_op() else {
                return Err(format!(
                    "the {:?} pipe cannot be put after {:?} pipe in the query [{self}]",
                    p.to_string(),
                    ps_str
                ));
            };
            match op {
                StatsTailOp::Keep => {
                    // This pipe doesn't change the set of fields.
                }
                StatsTailOp::OffsetLimit => {
                    if step > 0 {
                        return Err(format!(
                            "the {} pipe isn't allowed in range queries, since it cannot be applied individualley per each step; step={step}",
                            p.to_string()
                        ));
                    }
                    // limit and offset pipes do not change the set of fields,
                    // so they are allowed in instant queries.
                }
                StatsTailOp::RunningStats {
                    by_fields,
                    is_total,
                    result_names,
                } => {
                    // `| running_stats ...` pipe must contain the same
                    // labelFields as the preceding `stats` pipe.
                    //
                    // Allow `| total_stats ...` if it uses smaller `by (...)`
                    // list (subset of labels).
                    if !has_needed_fields_except_time(&by_fields, &label_fields) {
                        let allow_total_stats_by_subset =
                            is_total && has_only_known_fields(&by_fields, &label_fields);
                        if !allow_total_stats_by_subset {
                            return Err(format!(
                                "the {:?} must contain the same list of fields as `stats` pipe in the query [{self}]",
                                p.to_string()
                            ));
                        }
                    }
                    for f in &result_names {
                        add_to_metric_fields(&mut label_fields, &mut metric_fields, f);
                    }
                }
                StatsTailOp::Math { result_fields } => {
                    // Allow `| math ...` pipe, since it adds additional
                    // metrics to the given set of fields.
                    for f in &result_fields {
                        add_to_metric_fields(&mut label_fields, &mut metric_fields, f);
                    }
                }
                StatsTailOp::Fields { field_filters } => {
                    label_fields.retain(|f| prefix_filter::match_filters(&field_filters, f));
                    metric_fields.retain(|f| prefix_filter::match_filters(&field_filters, f));
                }
                StatsTailOp::Delete { field_filters } => {
                    label_fields.retain(|f| !prefix_filter::match_filters(&field_filters, f));
                    metric_fields.retain(|f| !prefix_filter::match_filters(&field_filters, f));
                }
                StatsTailOp::Copy { src, dst } => {
                    // Add copied fields to by(...) fields list.
                    for (f_src, f_dst) in src.iter().zip(dst.iter()) {
                        let label_fields_snapshot = label_fields.clone();
                        for f in &label_fields_snapshot {
                            if prefix_filter::match_filter(f_src, f) {
                                let dst_field_name = append_replace(f_src, f_dst, f);
                                add_to_label_fields(
                                    &mut label_fields,
                                    &mut metric_fields,
                                    &dst_field_name,
                                );
                            }
                        }

                        let metric_fields_snapshot: Vec<String> =
                            metric_fields.iter().cloned().collect();
                        for f in &metric_fields_snapshot {
                            if prefix_filter::match_filter(f_dst, f) {
                                metric_fields.remove(f);
                            }
                            if prefix_filter::match_filter(f_src, f) {
                                let dst_field_name = append_replace(f_src, f_dst, f);
                                add_to_metric_fields(
                                    &mut label_fields,
                                    &mut metric_fields,
                                    &dst_field_name,
                                );
                            }
                        }
                    }
                }
                StatsTailOp::Rename { src, dst } => {
                    // Update by(...) fields with dst fields
                    for (f_src, f_dst) in src.iter().zip(dst.iter()) {
                        let label_fields_copy = std::mem::take(&mut label_fields);
                        for f in &label_fields_copy {
                            if prefix_filter::match_filter(f_src, f) {
                                let dst_field_name = append_replace(f_src, f_dst, f);
                                add_to_label_fields(
                                    &mut label_fields,
                                    &mut metric_fields,
                                    &dst_field_name,
                                );
                            } else {
                                add_to_label_fields(&mut label_fields, &mut metric_fields, f);
                            }
                        }

                        let metric_fields_snapshot: Vec<String> =
                            metric_fields.iter().cloned().collect();
                        for f in &metric_fields_snapshot {
                            if prefix_filter::match_filter(f_dst, f) {
                                metric_fields.remove(f);
                            }
                            if prefix_filter::match_filter(f_src, f) {
                                metric_fields.remove(f);
                                let dst_field_name = append_replace(f_src, f_dst, f);
                                add_to_metric_fields(
                                    &mut label_fields,
                                    &mut metric_fields,
                                    &dst_field_name,
                                );
                            }
                        }
                    }
                }
                StatsTailOp::Format { result_field } => {
                    // Assume that `| format ...` pipe generates an additional
                    // by(...) label
                    add_to_label_fields(&mut label_fields, &mut metric_fields, &result_field);
                }
                StatsTailOp::UnpackJson { field_filters } => {
                    // Assume that `| unpack_json ... fields (...)` pipe
                    // generates additional by(...) labels from fields(...)
                    if field_filters.is_empty() || prefix_filter::match_all(&field_filters) {
                        return Err(format!(
                            "missing fields(...) after {:?} in the query [{self}]",
                            p.to_string()
                        ));
                    }
                    for f in &field_filters {
                        if prefix_filter::is_wildcard_filter(f) {
                            return Err(format!(
                                "fields(...) at {:?} cannot contain wildcard filter; got {f}; query [{self}]",
                                p.to_string()
                            ));
                        }
                        add_to_label_fields(&mut label_fields, &mut metric_fields, f);
                    }
                }
            }
        }

        if metric_fields.is_empty() {
            return Err(format!(
                "missing metric fields in the results of query [{self}]"
            ));
        }

        Ok(label_fields)
    }

    /// Port of Go `addByTimeFieldToStatsPipes`.
    fn add_by_time_field_to_stats_pipes(&mut self, step: i64, offset: i64) {
        for p in &mut self.pipes {
            p.stats_add_by_time_field(step, offset);
        }
    }

    /// Port of Go `Query.addPartitionByTime`.
    fn add_partition_by_time(&mut self, step: i64) {
        for p in &mut self.pipes {
            p.add_partition_by_time(step);
        }
    }

    /// Returns the set of fixed fields returned by the given query q
    /// (Go `GetFixedFields`).
    ///
    /// `None` is returned if it is impossible to detect the set of fields to
    /// return for the given q (Go returns `ok == false`).
    pub fn get_fixed_fields(&self) -> Option<Vec<String>> {
        let (mut fields, pipe_idx) = get_fixed_fields(&self.pipes)?;

        // fix the order of fields if sort pipe is present
        for p in &self.pipes[pipe_idx + 1..] {
            if let Some(adjusted) = p.sort_adjust_result_fields_order(&fields) {
                fields = adjusted;
            }
        }

        Some(fields)
    }

    /// Returns true if the query results have fixed order of fields
    /// (Go `IsFixedOutputFieldsOrder`).
    pub fn is_fixed_output_fields_order(&self) -> bool {
        for p in self.pipes.iter().rev() {
            if p.is_fixed_output_fields_order() {
                return true;
            }
            if p.subquery_is_fixed_output_fields_order() == Some(false) {
                return false;
            }
        }
        false
    }
}

/// Port of Go `getFixedFields` (free function): returns the fixed fields and
/// the index of the pipe that produced them.
fn get_fixed_fields(pipes: &[Box<dyn Pipe>]) -> Option<(Vec<String>, usize)> {
    for i in (0..pipes.len()).rev() {
        let p = &pipes[i];
        if p.fixed_fields_transparent() {
            // sort/limit/offset pipes do not change the fixed fields, so they
            // are allowed after `fields` and `stats`.
            continue;
        }
        return p.fixed_result_fields().map(|fields| (fields, i));
    }
    None
}

/// Port of Go `getLastPipeStatsIdx`.
fn get_last_pipe_stats_idx(pipes: &[Box<dyn Pipe>]) -> Option<usize> {
    (0..pipes.len())
        .rev()
        .find(|&i| pipes[i].stats_pipe_fields().is_some())
}

/// Port of Go `getUniqueResultName` (parser.go).
pub(crate) fn get_unique_result_name(result_name: &str, by_fields: &[String]) -> String {
    let mut name = result_name.to_string();
    while by_fields.iter().any(|f| f == &name) {
        name.push('s');
    }
    name
}

/// Go `addToLabelFields` closure in `GetStatsLabelsAddGroupingByTime`.
fn add_to_label_fields(
    label_fields: &mut Vec<String>,
    metric_fields: &mut HashSet<String>,
    f: &str,
) {
    if !label_fields.iter().any(|x| x == f) {
        label_fields.push(f.to_string());
    }
    metric_fields.remove(f);
}

/// Go `addToMetricFields` closure in `GetStatsLabelsAddGroupingByTime`.
fn add_to_metric_fields(
    label_fields: &mut Vec<String>,
    metric_fields: &mut HashSet<String>,
    f: &str,
) {
    if let Some(idx) = label_fields.iter().position(|x| x == f) {
        label_fields.remove(idx);
    }
    metric_fields.insert(f.to_string());
}

/// Port of Go `hasNeededFieldsExceptTime`.
fn has_needed_fields_except_time(fields: &[String], needed_fields: &[String]) -> bool {
    for f in needed_fields {
        if f == "_time" {
            continue;
        }
        if !fields.contains(f) {
            return false;
        }
    }

    for f in fields {
        if !needed_fields.contains(f) {
            return false;
        }
    }

    true
}

/// Port of Go `hasOnlyKnownFields`.
fn has_only_known_fields(fields: &[String], known_fields: &[String]) -> bool {
    fields.iter().all(|f| known_fields.contains(f))
}

/// String-returning wrapper around [`prefix_filter::append_replace`]
/// (Go `prefixfilter.AppendReplace`).
fn append_replace(src_filter: &str, dst_filter: &str, s: &str) -> String {
    let mut buf = Vec::new();
    prefix_filter::append_replace(&mut buf, src_filter, dst_filter, s);
    String::from_utf8_lossy(&buf).into_owned()
}

/// String-returning wrapper around
/// [`marshal_duration_string`](crate::values_encoder::marshal_duration_string)
/// (Go `marshalDurationString`).
fn duration_string(nsecs: i64) -> String {
    let mut buf = Vec::new();
    marshal_duration_string(&mut buf, nsecs);
    String::from_utf8_lossy(&buf).into_owned()
}
