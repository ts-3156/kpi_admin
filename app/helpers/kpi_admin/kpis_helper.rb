module KpiAdmin
  module KpisHelper
    def optional_conditions
      user_id_condition =
        case user_id_value
          when nil then ''
          when false then 'AND user_id = -1'
          when true then 'AND user_id != -1'
          else raise NotImplementedError
        end
      ego_surfing_condition =
        case ego_surfing_value
          when nil then ''
          when false then 'AND ego_surfing = 0'
          when true then 'AND ego_surfing = 1'
          else raise NotImplementedError
        end
      <<-"STR".strip_heredoc
      #{user_id_condition}
      #{ego_surfing_condition}
      #{action_values ? "AND action IN (#{action_values.join(',')})" : ''}
      #{device_type_values ? "AND device_type IN (#{device_type_values.join(',')})" : ''}
      #{channel_value ? "AND channel LIKE '%#{channel_value}%'" : ''}
      STR
    end

    def placeholder_values(days)
      {
        start: days.first.beginning_of_day,
        end: days.last.end_of_day,
        label: days.last.beginning_of_day
      }
    end

    def show_sql(type, days)
      ActiveRecord::Base.send(:sanitize_sql_array, [send("#{type}_sql"), placeholder_values(days)])
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
