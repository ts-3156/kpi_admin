module KpiAdmin
  module KpisHelper
    def boolean_condition(name, value)
      case value
        when nil then ''
        when false then "AND #{name} = 0"
        when true then "AND #{name} = 1"
        else raise NotImplementedError
      end
    end

    def optional_common_conditions
      user_id_condition =
        case user_id_value
          when nil then ''
          when false then 'AND user_id = -1'
          when true then 'AND user_id != -1'
          else raise NotImplementedError
        end
      <<-"STR".strip_heredoc
      #{user_id_condition}
      #{device_type_values ? "AND device_type IN (#{device_type_values.join(',')})" : ''}
      #{channel_value ? "AND channel LIKE '%#{channel_value}%'" : ''}
      STR
    end

    def optional_search_logs_conditions
      <<-"STR".strip_heredoc
      #{boolean_condition(:ego_surfing, ego_surfing_value)}
      #{action_values ? "AND action IN (#{action_values.join(',')})" : ''}
      STR
    end

    def optional_background_search_logs_conditions
      <<-"STR".strip_heredoc
      #{boolean_condition(:status, status_value)}
      #{boolean_condition(:auto, auto_value)}
      STR
    end

    def optional_sign_in_logs_conditions
      <<-"STR".strip_heredoc
      #{context_values ? "AND context IN (#{context_values.join(',')})" : ''}
      STR
    end

    def optional_modal_open_logs_conditions
      <<-"STR".strip_heredoc
      #{name_values ? "AND name IN (#{name_values.join(',')})" : ''}
      STR
    end

    def placeholder_values(days)
      {
        start: days.first.beginning_of_day,
        end: days.last.end_of_day,
        label: days.last.beginning_of_day
      }
    end

    def datetime_label
      date_array[sequence_number].last.beginning_of_day
    end

    def first_datetime
      date_array[sequence_number].first.beginning_of_day
    end

    def last_datetime
      date_array[sequence_number].last.end_of_day
    end

    def exec_sql(klass, sql)
      if sequence_number
        days = date_array[sequence_number]
        klass.find_by_sql([sql, placeholder_values(days)])
      else
        date_array.map { |days| klass.find_by_sql([sql, placeholder_values(days)]) }.flatten
      end
    end
  end
end
