module KpiAdmin
  module Kpis
    module ModalOpenHelper
      def fetch_modal_open_uu
        result = exec_sql(ModalOpenLog, modal_open_uu_sql)
        %i(total guest login).map do |legend|
          {
            name: legend,
            data: result.map { |r| [to_msec_unixtime(r.date), r.send(legend)] }
          }
        end
      end

      def modal_open_uu_sql
        <<-"SQL".strip_heredoc
      SELECT
        :label date,
        count(DISTINCT session_id) total,
        count(DISTINCT if(user_id = -1, session_id, NULL)) guest,
        count(DISTINCT if(user_id != -1, session_id, NULL)) login
      FROM modal_open_logs
      WHERE
        created_at BETWEEN :start AND :end
        AND device_type NOT IN ('crawler', 'UNKNOWN')
        #{optional_common_conditions}
        #{optional_modal_open_logs_conditions}
        SQL
      end

      def fetch_modal_open_num
        result = exec_sql(ModalOpenLog, modal_open_num_sql)
        %i(total guest login).map do |legend|
          {
            name: legend,
            data: result.map { |r| [to_msec_unixtime(r.date), r.send(legend)] }
          }
        end
      end

      def modal_open_num_sql
        <<-"SQL".strip_heredoc
      SELECT
        :label date,
        count(*) total,
        count(if(user_id = -1, 1, NULL)) guest,
        count(if(user_id != -1, 1, NULL)) login
      FROM modal_open_logs
      WHERE
        created_at BETWEEN :start AND :end
        AND device_type NOT IN ('crawler', 'UNKNOWN')
        #{optional_common_conditions}
        #{optional_modal_open_logs_conditions}
        SQL
      end

      %i(device_type referer unified_referer channel unified_channel).each do |type|
        is_visible = -> legend do
          case type
            when :unified_referer then %w(EGOTTER).exclude?(legend)
            else true
          end
        end

        define_method("fetch_modal_open_uu_per_#{type}") do
          result = exec_sql(ModalOpenLog, send("modal_open_uu_per_#{type}_sql"))
          result.map{|r| r.send(type) }.reject { |a| a == 'NULL' }.sort.uniq.map do |legend|
            {
              name: legend,
              data: result.select { |r| r.send(type) == legend }.map { |r| [to_msec_unixtime(r.date), r.total] },
              visible: is_visible.call(legend)
            }
          end
        end

        define_method("fetch_modal_open_num_per_#{type}") do
          result = exec_sql(ModalOpenLog, send("modal_open_num_per_#{type}_sql"))
          result.map { |r| r.send(type) }.reject { |a| a == 'NULL' }.sort.uniq.map do |legend|
            {
              name: legend,
              data: result.select { |r| r.send(type) == legend }.map { |r| [to_msec_unixtime(r.date), r.total] },
              visible: is_visible.call(legend)
            }
          end
        end

        define_method("modal_open_uu_per_#{type}_sql") do
          <<-"SQL".strip_heredoc
            SELECT
              :label date,
              #{type},
              count(DISTINCT session_id) total
            FROM modal_open_logs
            WHERE
              created_at BETWEEN :start AND :end
              AND device_type NOT IN ('crawler', 'UNKNOWN')
              #{optional_common_conditions}
              #{optional_modal_open_logs_conditions}
            GROUP BY #{type}
          SQL
        end

        define_method("modal_open_num_per_#{type}_sql") do
          <<-"SQL".strip_heredoc
            SELECT
              :label date,
              #{type},
              count(*) total
            FROM modal_open_logs
            WHERE
              created_at BETWEEN :start AND :end
              AND device_type NOT IN ('crawler', 'UNKNOWN')
              #{optional_common_conditions}
              #{optional_modal_open_logs_conditions}
            GROUP BY #{type}
          SQL
        end
      end
    end
  end
end
