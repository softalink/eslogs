//! Port of EsLogs `lib/logstorage` (storage engine, LogsQL query
//! language) and `lib/prefixfilter`. One module per upstream Go file; see
//! `docs/LOGSTORAGE_PLAN.md` for layering and `docs/CONVENTIONS.md` for rules.

// Layer 0 — primitives
pub mod arena;
pub mod bitmap;
pub mod bloomfilter;
pub mod cache;
pub mod chunked_allocator;
pub mod color_sequence;
pub mod consts;
pub mod filenames;
pub mod hash128;
pub mod hash_tokenizer;
pub mod prefix_filter;
pub mod stringbucket;
pub mod tokenizer;
pub mod u128;

// Layer 1 — value encoding & parsing helpers
pub mod encoding;
pub mod in_values;
pub mod json_parser;
pub mod json_scanner;
pub mod logfmt_parser;
pub mod pattern;
pub mod pattern_matcher;
pub mod rows;
pub mod stream_tags;
pub mod syslog_parser;
pub mod tenant_id;
pub mod values_encoder;

// Layer 2 — block format
pub mod block;
pub mod block_data;
pub mod block_header;
pub mod block_stream_merger;
pub mod block_stream_reader;
pub mod block_stream_writer;
pub mod column_names;
pub mod inmemory_part;
pub mod log_rows;
pub mod part;
pub mod part_header;
pub mod stream_id;

// Layer 3 — storage engine
pub mod datadb;
pub mod delete_task;
pub mod indexdb;
pub mod partition;
pub mod query_stats;
pub mod storage;
pub mod stream_filter;

// Layer 4 — LogsQL query engine (substrate)
pub mod block_result;
pub mod block_search;
pub mod filter;

// Layer 4 — LogsQL filters
pub mod filter_and;
pub mod filter_any_case_phrase;
pub mod filter_any_case_prefix;
pub mod filter_contains_all;
pub mod filter_contains_any;
pub mod filter_contains_common_case;
pub mod filter_day_range;
pub mod filter_eq_field;
pub mod filter_equals_common_case;
pub mod filter_exact;
pub mod filter_exact_prefix;
pub mod filter_generic;
pub mod filter_in;
pub mod filter_ipv4_range;
pub mod filter_ipv6_range;
pub mod filter_json_array_contains_any;
pub mod filter_le_field;
pub mod filter_len_range;
pub mod filter_noop;
pub mod filter_not;
pub mod filter_or;
pub mod filter_pattern_match;
pub mod filter_phrase;
pub mod filter_prefix;
pub mod filter_range;
pub mod filter_regexp;
pub mod filter_sequence;
pub mod filter_stream;
pub mod filter_stream_id;
pub mod filter_string_range;
pub mod filter_substring;
pub mod filter_time;
pub mod filter_value_type;
pub mod filter_week_range;

// Layer 6 — stats functions
pub mod running_stats_count;
pub mod running_stats_first;
pub mod running_stats_last;
pub mod running_stats_max;
pub mod running_stats_min;
pub mod running_stats_sum;
pub mod stats;
pub mod stats_any;
pub mod stats_avg;
pub mod stats_count;
pub mod stats_count_empty;
pub mod stats_count_uniq;
pub mod stats_count_uniq_hash;
pub mod stats_field_max;
pub mod stats_field_min;
pub mod stats_histogram;
pub mod stats_json_values;
pub mod stats_json_values_sorted;
pub mod stats_json_values_topk;
pub mod stats_max;
pub mod stats_median;
pub mod stats_min;
pub mod stats_quantile;
pub mod stats_rate;
pub mod stats_rate_sum;
pub mod stats_row_any;
pub mod stats_row_max;
pub mod stats_row_min;
pub mod stats_stddev;
pub mod stats_sum;
pub mod stats_sum_len;
pub mod stats_uniq_values;
pub mod stats_values;

// Layer 5 — pipes
pub mod pipe;
pub mod pipe_block_stats;
pub mod pipe_blocks_count;
pub mod pipe_coalesce;
pub mod pipe_collapse_nums;
pub mod pipe_copy;
pub mod pipe_decolorize;
pub mod pipe_delete;
pub mod pipe_drop_empty_fields;
pub mod pipe_extract;
pub mod pipe_extract_regexp;
pub mod pipe_facets;
pub mod pipe_field_names;
pub mod pipe_field_values;
pub mod pipe_field_values_local;
pub mod pipe_fields;
pub mod pipe_filter;
pub mod pipe_first;
pub mod pipe_format;
pub mod pipe_generate_sequence;
pub mod pipe_hash;
pub mod pipe_join;
pub mod pipe_json_array_len;
pub mod pipe_last;
pub mod pipe_len;
pub mod pipe_limit;
pub mod pipe_math;
pub mod pipe_offset;
pub mod pipe_pack;
pub mod pipe_pack_json;
pub mod pipe_pack_logfmt;
pub mod pipe_query_stats;
pub mod pipe_query_stats_local;
pub mod pipe_rename;
pub mod pipe_replace;
pub mod pipe_replace_regexp;
pub mod pipe_running_stats;
pub mod pipe_sample;
pub mod pipe_set_stream_fields;
pub mod pipe_sort;
pub mod pipe_sort_topk;
pub mod pipe_split;
pub mod pipe_stats;
pub mod pipe_stream_context;
pub mod pipe_time_add;
pub mod pipe_top;
pub mod pipe_total_stats;
pub mod pipe_union;
pub mod pipe_uniq;
pub mod pipe_uniq_local;
pub mod pipe_unpack;
pub mod pipe_unpack_json;
pub mod pipe_unpack_logfmt;
pub mod pipe_unpack_syslog;
pub mod pipe_unpack_words;
pub mod pipe_unroll;
pub mod pipe_update;

// Layer 4 finalize — parser, search, wiring
pub mod hits_map;
pub mod if_filter;
pub mod net_query_runner;
pub mod parser;
pub mod storage_search;
