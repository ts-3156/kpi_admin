module KpiAdmin
  module Kpis
    module PvUuHelper
      def fetch_uu
        result = exec_sql(SearchLog, uu_sql)
        %i(total guest login).map do |legend|
          {
            name: legend,
            data: result.map { |r| [to_msec_unixtime(r.date), r.send(legend)] }
          }
        end
      end

      def uu_sql
        <<-"SQL".strip_heredoc
      SELECT
        :label date,
        count(DISTINCT session_id) total,
        count(DISTINCT if(user_id = -1, session_id, NULL)) guest,
        count(DISTINCT if(user_id != -1, session_id, NULL)) login
      FROM tmp_search_logs
      WHERE
        created_at BETWEEN :start AND :end
        AND device_type NOT IN ('crawler', 'UNKNOWN')
        #{optional_common_conditions}
        #{optional_search_logs_conditions}
        SQL
      end

      def fetch_pv
        result = exec_sql(SearchLog, pv_sql)
        %i(total guest login).map do |legend|
          {
            name: legend,
            data: result.map { |r| [to_msec_unixtime(r.date), r.send(legend)] }
          }
        end
      end

      def pv_sql
        <<-"SQL".strip_heredoc
      SELECT
        :label date,
        count(*) total,
        count(if(user_id = -1, 1, NULL)) guest,
        count(if(user_id != -1, 1, NULL)) login
      FROM tmp_search_logs
      WHERE
        created_at BETWEEN :start AND :end
        AND device_type NOT IN ('crawler', 'UNKNOWN')
        #{optional_common_conditions}
        #{optional_search_logs_conditions}
        SQL
      end

      %i(action device_type referer unified_referer channel unified_channel).each do |type|
        is_visible = -> legend do
          case type
            when :action then %w(new removing removed).include?(legend)
            when :unified_referer then %w(EGOTTER).exclude?(legend)
            else true
          end
        end

        define_method("fetch_uu_per_#{type}") do
          result = exec_sql(SearchLog, send("uu_per_#{type}_sql"))
          result.map{|r| r.send(type) }.reject { |a| a == 'NULL' }.sort.uniq.map do |legend|
            {
              name: legend,
              data: result.select { |r| r.send(type) == legend }.map { |r| [to_msec_unixtime(r.date), r.total] },
              visible: is_visible.call(legend)
            }
          end
        end

        define_method("fetch_pv_per_#{type}") do
          result = exec_sql(SearchLog, send("pv_per_#{type}_sql"))
          result.map { |r| r.send(type) }.reject { |a| a == 'NULL' }.sort.uniq.map do |legend|
            {
              name: legend,
              data: result.select { |r| r.send(type) == legend }.map { |r| [to_msec_unixtime(r.date), r.total] },
              visible: is_visible.call(legend)
            }
          end
        end

        define_method("uu_per_#{type}_sql") do
          <<-"SQL".strip_heredoc
            SELECT
              :label date,
              #{type},
              count(DISTINCT session_id) total
            FROM tmp_search_logs
            WHERE
              created_at BETWEEN :start AND :end
              AND device_type NOT IN ('crawler', 'UNKNOWN')
              #{optional_common_conditions}
              #{optional_search_logs_conditions}
            GROUP BY #{type}
          SQL
        end

        define_method("pv_per_#{type}_sql") do
          <<-"SQL".strip_heredoc
            SELECT
              :label date,
              #{type},
              count(*) total
            FROM tmp_search_logs
            WHERE
              created_at BETWEEN :start AND :end
              AND device_type NOT IN ('crawler', 'UNKNOWN')
              AND action != 'waiting'
              #{optional_common_conditions}
              #{optional_search_logs_conditions}
            GROUP BY #{type}
          SQL
        end
      end

      def fetch_new_user
        result = exec_sql(User, new_user_sql)
        %i(total).map do |legend|
          {
            name: legend,
            data: result.map { |r| [to_msec_unixtime(r.date), r.send(legend)] }
          }
        end
      end

      def new_user_sql
        <<-"SQL".strip_heredoc
      SELECT
        :label date,
        count(*) total
      FROM users
      WHERE
        created_at BETWEEN :start AND :end
        SQL
      end
    end
  end
end
