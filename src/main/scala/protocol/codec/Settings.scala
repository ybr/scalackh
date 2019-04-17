package scalackh.protocol.codec

import scalackh.math.UInt64
import scalackh.protocol.ClickhouseClientException
import scalackh.protocol.codec.DefaultEncoders.writeString
import scalackh.protocol.codec.LEB128.{writeVarInt, writeVarLong}

sealed trait SettingEncoder
case class UInt64SettingEncoder(encoder: Encoder[UInt64]) extends SettingEncoder
case class BooleanSettingEncoder(encoder: Encoder[Boolean]) extends SettingEncoder
case class StringSettingEncoder(encoder: Encoder[String]) extends SettingEncoder
case class FloatSettingEncoder(encoder: Encoder[Float]) extends SettingEncoder

object SettingEncoders {
  val settingUInt64Encoder: UInt64SettingEncoder = UInt64SettingEncoder(Encoder { (setting, buf) =>
    writeVarLong(setting.unsafeLong, buf)
  })

  val settingBoolEncoder: BooleanSettingEncoder = BooleanSettingEncoder(Encoder { (bool, buf) =>
    writeVarInt(if(bool) 1 else 0, buf)
  })

  val settingStringEncoder: StringSettingEncoder = StringSettingEncoder(Encoder { (str, buf) =>
    writeString(str, buf)
  })

  // Float is written in string representation
  val settingFloatEncoder: FloatSettingEncoder = FloatSettingEncoder(Encoder { (float, buf) =>
    writeString(float.toString, buf)
  })

  // https://github.com/mymarilyn/clickhouse-driver/blob/master/clickhouse_driver/settings/available.py
  val settingEncoders: Map[String, SettingEncoder] = Map(
    "min_compress_block_size" -> settingUInt64Encoder,
    "max_compress_block_size" -> settingUInt64Encoder,
    "max_block_size" -> settingUInt64Encoder,
    "max_insert_block_size" -> settingUInt64Encoder,
    "min_insert_block_size_rows" -> settingUInt64Encoder,
    "min_insert_block_size_bytes" -> settingUInt64Encoder,
    "max_partitions_per_insert_block" -> settingUInt64Encoder,
    "max_threads" -> settingUInt64Encoder,
    "max_read_buffer_size" -> settingUInt64Encoder,
    "max_distributed_connections" -> settingUInt64Encoder,
    "max_query_size" -> settingUInt64Encoder,
    "interactive_delay" -> settingUInt64Encoder,
    "connect_timeout" -> settingUInt64Encoder,
    "connect_timeout_with_failover_ms" -> settingUInt64Encoder,
    "receive_timeout" -> settingUInt64Encoder,
    "send_timeout" -> settingUInt64Encoder,
    "queue_max_wait_ms" -> settingUInt64Encoder,
    "poll_interval" -> settingUInt64Encoder,
    "distributed_connections_pool_size" -> settingUInt64Encoder,
    "connections_with_failover_max_tries" -> settingUInt64Encoder,
    "extremes" -> settingBoolEncoder,
    "use_uncompressed_cache" -> settingBoolEncoder,
    "replace_running_query" -> settingBoolEncoder,
    "background_pool_size" -> settingUInt64Encoder,
    "background_schedule_pool_size" -> settingUInt64Encoder,

    "distributed_directory_monitor_sleep_time_ms" -> settingUInt64Encoder,

    "distributed_directory_monitor_batch_inserts" -> settingBoolEncoder,

    "optimize_move_to_prewhere" -> settingBoolEncoder,

    "replication_alter_partitions_sync" -> settingUInt64Encoder,
    "replication_alter_columns_timeout" -> settingUInt64Encoder,

    "load_balancing" -> settingStringEncoder,

    "totals_mode" -> settingStringEncoder,
    "totals_auto_threshold" -> settingFloatEncoder,

    "compile" -> settingBoolEncoder,
    "compile_expressions" -> settingBoolEncoder,
    "min_count_to_compile" -> settingUInt64Encoder,
    "group_by_two_level_threshold" -> settingUInt64Encoder,
    "group_by_two_level_threshold_bytes" -> settingUInt64Encoder,
    "distributed_aggregation_memory_efficient" -> settingBoolEncoder,
    "aggregation_memory_efficient_merge_threads" -> settingUInt64Encoder,

    "max_parallel_replicas" -> settingUInt64Encoder,
    "parallel_replicas_count" -> settingUInt64Encoder,
    "parallel_replica_offset" -> settingUInt64Encoder,

    "skip_unavailable_shards" -> settingBoolEncoder,

    "distributed_group_by_no_merge" -> settingBoolEncoder,

    "merge_tree_min_rows_for_concurrent_read" -> settingUInt64Encoder,
    "merge_tree_min_rows_for_seek" -> settingUInt64Encoder,
    "merge_tree_coarse_index_granularity" -> settingUInt64Encoder,
    "merge_tree_max_rows_to_use_cache" -> settingUInt64Encoder,

    "merge_tree_uniform_read_distribution" -> settingBoolEncoder,

    "mysql_max_rows_to_insert" -> settingUInt64Encoder,

    "optimize_min_equality_disjunction_chain_length" -> settingUInt64Encoder,

    "min_bytes_to_use_direct_io" -> settingUInt64Encoder,

    "force_index_by_date" -> settingBoolEncoder,
    "force_primary_key" -> settingBoolEncoder,

    "mark_cache_min_lifetime" -> settingUInt64Encoder,

    "max_streams_to_max_threads_ratio" -> settingFloatEncoder,

    "network_compression_method" -> settingStringEncoder,

    "network_zstd_compression_level" -> settingUInt64Encoder,

    "priority" -> settingUInt64Encoder,

    "log_queries" -> settingBoolEncoder,

    "log_queries_cut_to_length" -> settingUInt64Encoder,

    "distributed_product_mode" -> settingStringEncoder,

    "max_concurrent_queries_for_user" -> settingUInt64Encoder,

    "insert_deduplicate" -> settingBoolEncoder,

    "insert_quorum" -> settingUInt64Encoder,
    "insert_quorum_timeout" -> settingUInt64Encoder,
    "select_sequential_consistency" -> settingUInt64Encoder,
    "table_function_remote_max_addresses" -> settingUInt64Encoder,
    "read_backoff_min_latency_ms" -> settingUInt64Encoder,
    "read_backoff_max_throughput" -> settingUInt64Encoder,
    "read_backoff_min_interval_between_events_ms" -> settingUInt64Encoder,
    "read_backoff_min_events" -> settingUInt64Encoder,

    "memory_tracker_fault_probability" -> settingFloatEncoder,

    "enable_http_compression" -> settingBoolEncoder,
    "http_zlib_compression_level" -> settingUInt64Encoder,

    "http_native_compression_disable_checksumming_on_decompress" -> settingBoolEncoder,

    "count_distinct_implementation" -> settingStringEncoder,

    "output_format_write_statistics" -> settingBoolEncoder,

    "add_http_cors_header" -> settingBoolEncoder,

    "input_format_skip_unknown_fields" -> settingBoolEncoder,
    "input_format_import_nested_json" -> settingBoolEncoder,
    "input_format_values_interpret_expressions" -> settingBoolEncoder,

    "output_format_json_quote_64bit_integers" -> settingBoolEncoder,

    "output_format_json_quote_denormals" -> settingBoolEncoder,

    "output_format_json_escape_forward_slashes" -> settingBoolEncoder,

    "output_format_pretty_max_rows" -> settingUInt64Encoder,
    "output_format_pretty_max_column_pad_width" -> settingUInt64Encoder,
    "output_format_pretty_color" -> settingBoolEncoder,

    "use_client_time_zone" -> settingBoolEncoder,

    "send_progress_in_http_headers" -> settingBoolEncoder,

    "http_headers_progress_interval_ms" -> settingUInt64Encoder,

    "fsync_metadata" -> settingBoolEncoder,

    "input_format_allow_errors_num" -> settingUInt64Encoder,
    "input_format_allow_errors_ratio" -> settingFloatEncoder,

    "join_use_nulls" -> settingBoolEncoder,
    "join_default_strictness" -> settingStringEncoder,
    "preferred_block_size_bytes" -> settingUInt64Encoder,

    "max_replica_delay_for_distributed_queries" -> settingUInt64Encoder,
    "fallback_to_stale_replicas_for_distributed_queries" -> settingBoolEncoder,
    "preferred_max_column_in_block_size_bytes" -> settingUInt64Encoder,

    "insert_distributed_sync" -> settingBoolEncoder,
    "insert_distributed_timeout" -> settingUInt64Encoder,
    "distributed_ddl_task_timeout" -> settingUInt64Encoder,
    "stream_flush_interval_ms" -> settingUInt64Encoder,
    "format_schema" -> settingStringEncoder,
    "insert_allow_materialized_columns" -> settingBoolEncoder,
    "http_connection_timeout" -> settingUInt64Encoder,
    "http_send_timeout" -> settingUInt64Encoder,
    "http_receive_timeout" -> settingUInt64Encoder,
    "optimize_throw_if_noop" -> settingBoolEncoder,
    "use_index_for_in_with_subqueries" -> settingBoolEncoder,
    "empty_result_for_aggregation_by_empty_set" -> settingBoolEncoder,
    "allow_distributed_ddl" -> settingBoolEncoder,
    "odbc_max_field_size" -> settingUInt64Encoder,

    // Limits
    "max_rows_to_read" -> settingUInt64Encoder,
    "max_bytes_to_read" -> settingUInt64Encoder,
    "read_overflow_mode" -> settingStringEncoder,

    "max_rows_to_group_by" -> settingUInt64Encoder,
    "group_by_overflow_mode" -> settingStringEncoder,
    "max_bytes_before_external_group_by" -> settingUInt64Encoder,

    "max_rows_to_sort" -> settingUInt64Encoder,
    "max_bytes_to_sort" -> settingUInt64Encoder,
    "sort_overflow_mode" -> settingStringEncoder,
    "max_bytes_before_external_sort" -> settingUInt64Encoder,
    "max_bytes_before_remerge_sort" -> settingUInt64Encoder,
    "max_result_rows" -> settingUInt64Encoder,
    "max_result_bytes" -> settingUInt64Encoder,
    "result_overflow_mode" -> settingStringEncoder,

    "max_execution_time" -> settingUInt64Encoder,
    "timeout_overflow_mode" -> settingStringEncoder,

    "min_execution_speed" -> settingUInt64Encoder,
    "timeout_before_checking_execution_speed" -> settingUInt64Encoder,

    "max_columns_to_read" -> settingUInt64Encoder,
    "max_temporary_columns" -> settingUInt64Encoder,
    "max_temporary_non_const_columns" -> settingUInt64Encoder,

    "max_subquery_depth" -> settingUInt64Encoder,
    "max_pipeline_depth" -> settingUInt64Encoder,
    "max_ast_depth" -> settingUInt64Encoder,
    "max_ast_elements" -> settingUInt64Encoder,
    "max_expanded_ast_elements" -> settingUInt64Encoder,

    "readonly" -> settingUInt64Encoder,

    "max_rows_in_set" -> settingUInt64Encoder,
    "max_bytes_in_set" -> settingUInt64Encoder,
    "set_overflow_mode" -> settingStringEncoder,

    "max_rows_in_join" -> settingUInt64Encoder,
    "max_bytes_in_join" -> settingUInt64Encoder,
    "join_overflow_mode" -> settingStringEncoder,

    "max_rows_to_transfer" -> settingUInt64Encoder,
    "max_bytes_to_transfer" -> settingUInt64Encoder,
    "transfer_overflow_mode" -> settingStringEncoder,

    "max_rows_in_distinct" -> settingUInt64Encoder,
    "max_bytes_in_distinct" -> settingUInt64Encoder,
    "distinct_overflow_mode" -> settingStringEncoder,

    "max_memory_usage" -> settingUInt64Encoder,
    "max_memory_usage_for_user" -> settingUInt64Encoder,
    "max_memory_usage_for_all_queries" -> settingUInt64Encoder,

    "max_network_bandwidth" -> settingUInt64Encoder,
    "max_network_bytes" -> settingUInt64Encoder,
    "max_network_bandwidth_for_user" -> settingUInt64Encoder,
    "max_network_bandwidth_for_all_users" -> settingUInt64Encoder,
    "format_csv_delimiter" -> settingStringEncoder,
    "format_csv_allow_single_quotes" -> settingBoolEncoder,
    "format_csv_allow_double_quotes" -> settingBoolEncoder,

    "enable_conditional_computation" -> settingUInt64Encoder,

    "date_time_input_format" -> settingStringEncoder,
    "log_profile_events" -> settingBoolEncoder,
    "log_query_settings" -> settingBoolEncoder,
    "log_query_threads" -> settingBoolEncoder,
    "send_logs_level" -> settingStringEncoder,
    "enable_optimize_predicate_expression" -> settingBoolEncoder,
    "low_cardinality_max_dictionary_size" -> settingUInt64Encoder,
    "low_cardinality_use_single_dictionary_for_part" -> settingBoolEncoder,
    "allow_experimental_low_cardinality_type" -> settingBoolEncoder,
    "allow_experimental_decimal_type" -> settingBoolEncoder,
    "decimal_check_overflow" -> settingBoolEncoder,
    "prefer_localhost_replica" -> settingBoolEncoder,
    "max_fetch_partition_retries_count" -> settingUInt64Encoder,
    "asterisk_left_columns_only" -> settingBoolEncoder,
    "http_max_multipart_form_data_size" -> settingUInt64Encoder,
    "calculate_text_stack_trace" -> settingBoolEncoder,

    "allow_ddl" -> settingBoolEncoder,
    "parallel_view_processing" -> settingBoolEncoder
  )

  def settingSpecification(sencoder: SettingEncoder): String = sencoder match {
    case UInt64SettingEncoder(_) => "UInt64"
    case BooleanSettingEncoder(_) => "Boolean"
    case StringSettingEncoder(_) => "String"
    case FloatSettingEncoder(_) => "Float"
  }

  val settingsEncoder: Encoder[Map[String, Any]] = Encoder { (settings, buf) =>
    settings.foreach { case (key, value) =>
      settingEncoders.get(key).fold(throw new ClickhouseClientException(s"Settings ${key} is not supported")) { settingEncoder =>
        writeString(key, buf)
        (settingEncoder, value) match {
          case (UInt64SettingEncoder(encoder), uint64: UInt64) => encoder.write(uint64, buf)
          case (BooleanSettingEncoder(encoder), bool: Boolean) => encoder.write(bool, buf)
          case (StringSettingEncoder(encoder), str: String) => encoder.write(str, buf)
          case (FloatSettingEncoder(encoder), float: Float) => encoder.write(float, buf)
          case _ => throw new ClickhouseClientException(s"Settings value ${value}:${value.getClass.getName} does not match setting specification ${settingSpecification(settingEncoder)}")
        }
      }
    }
    writeString("", buf) // end of settings
  }
}