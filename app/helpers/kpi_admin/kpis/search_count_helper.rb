module KpiAdmin
  module Kpis
    module SearchCountHelper
      def fetch_search_uu
        result = exec_sql(SearchLog, search_uu_sql)
        %i(total guest login).map do |legend|
          {
            name: legend,
            data: result.map { |r| [to_msec_unixtime(r.date), r.send(legend)] },
          }
        end
      end

      def search_uu_sql
        <<-"SQL".strip_heredoc
      SELECT
        :label date,
        count(DISTINCT session_id) total,
        count(DISTINCT if(user_id = -1, session_id, NULL)) guest,
        count(DISTINCT if(user_id != -1, session_id, NULL)) login
      FROM search_logs
      WHERE
        created_at BETWEEN :start AND :end
        AND device_type NOT IN ('crawler', 'UNKNOWN')
        AND action = 'create'
        #{optional_common_conditions}
        #{optional_search_logs_conditions}
        SQL
      end

      def fetch_search_num
        result = exec_sql(SearchLog, search_num_sql)
        %i(guest login).map do |legend|
          {
            name: legend,
            data: result.map { |r| [to_msec_unixtime(r.date), r.send(legend)] },
          }
        end + fetch_search_num_verification
      end

      def search_num_sql
        <<-"SQL".strip_heredoc
      SELECT
        :label date,
        count(if(user_id = -1, 1, NULL)) guest,
        count(if(user_id != -1, 1, NULL)) login
      FROM search_logs
      WHERE
        created_at BETWEEN :start AND :end
        AND device_type NOT IN ('crawler', 'UNKNOWN')
        AND action = 'create'
        #{optional_common_conditions}
        #{optional_search_logs_conditions}
        SQL
      end

      def fetch_search_num_verification
        result = exec_sql(BackgroundSearchLog, search_num_verification_sql)
        %i(guest_verif login_verif).map do |legend|
          {
            name: legend,
            data: result.map { |r| [to_msec_unixtime(r.date), r.send(legend)] }
          }
        end
      end

      def search_num_verification_sql
        <<-"SQL".strip_heredoc
      SELECT
        :label date,
        count(if(user_id = -1, 1, NULL)) guest_verif,
        count(if(user_id != -1, 1, NULL)) login_verif
      FROM background_search_logs
      WHERE
        created_at BETWEEN :start AND :end
        AND device_type NOT IN ('crawler', 'UNKNOWN')
        #{optional_common_conditions}
        #{optional_background_search_logs_conditions}
        SQL
      end

      def fetch_search_num_per_uu
        result = exec_sql(BackgroundSearchLog, search_num_per_uu_sql)
        %i(guest login).uniq.sort.map do |legend|
          {
            name: legend,
            data: result.map { |r| [to_msec_unixtime(r.date), r.send(legend).to_f] }
          }
        end
      end

      def search_num_per_uu_sql
        <<-"SQL".strip_heredoc
      SELECT
        :label date,
        if(sum(a.session_id) = 0, 0, sum(b.guest_num) / sum(a.guest_uu)) guest,
        if(sum(a.session_id) = 0, 0, sum(b.login_num) / sum(a.login_uu)) login
      FROM (
        SELECT
          session_id,
          count(DISTINCT if(user_id = -1, session_id, NULL)) guest_uu,
          count(DISTINCT if(user_id != -1, session_id, NULL)) login_uu
        FROM search_logs
        WHERE
          created_at BETWEEN :start AND :end
          AND device_type NOT IN ('crawler', 'UNKNOWN')
          #{optional_common_conditions}
          #{optional_search_logs_conditions}
        GROUP BY
          session_id
      ) a LEFT OUTER JOIN (
        SELECT
          session_id,
          count(if(user_id = -1, 1, NULL)) guest_num,
          count(if(user_id != -1, 1, NULL)) login_num
        FROM background_search_logs
        WHERE
          created_at BETWEEN :start AND :end
          AND device_type NOT IN ('crawler', 'UNKNOWN')
          #{optional_common_conditions}
          #{optional_background_search_logs_conditions}
        GROUP BY
          session_id
      ) b ON (a.session_id = b.session_id)
        SQL
      end

      def search_uu_per_action_sql
        <<-"SQL".strip_heredoc
      SELECT
        :label date,
        CASE
          WHEN referer regexp '^http://(www\.)?egotter\.com/?$' THEN 'new'
          WHEN referer regexp '^http://(www\.)?egotter\.com/searches' THEN 'results'
          WHEN referer = '' THEN 'direct'
          ELSE 'others'
        END action,
        count(DISTINCT session_id) total
      FROM background_search_logs
      WHERE
        created_at BETWEEN :start AND :end
        AND device_type NOT IN ('crawler', 'UNKNOWN')
        #{optional_common_conditions}
        #{optional_background_search_logs_conditions}
      GROUP BY
        action
        SQL
      end

      def search_num_per_action_sql
        <<-"SQL".strip_heredoc
      SELECT
        :label date,
        CASE
          WHEN referer regexp '^http://(www\.)?egotter\.com/?$' THEN 'new'
          WHEN referer regexp '^http://(www\.)?egotter\.com/searches' THEN 'results'
          WHEN referer = '' THEN 'direct'
          ELSE 'others'
        END action,
        count(*) total
      FROM background_search_logs
      WHERE
        created_at BETWEEN :start AND :end
        AND device_type NOT IN ('crawler', 'UNKNOWN')
        #{optional_common_conditions}
        #{optional_background_search_logs_conditions}
      GROUP BY
        action
        SQL
      end

      def search_num_per_uu_per_action_sql
        <<-"SQL".strip_heredoc
      SELECT
        :label date,
        if(b.session_id IS NULL, 'NULL', b.action) action,
        if(count(a.session_id) = 0, 0, sum(b.count) / count(a.session_id)) rate
      FROM (
        SELECT DISTINCT
          session_id
        FROM search_logs
        WHERE
          created_at BETWEEN :start AND :end
          AND device_type NOT IN ('crawler', 'UNKNOWN')
          #{optional_common_conditions}
          #{optional_search_logs_conditions}
      ) a LEFT OUTER JOIN (
        SELECT
          session_id,
          CASE
            WHEN referer regexp '^http://(www\.)?egotter\.com/?$' THEN 'new'
            WHEN referer regexp '^http://(www\.)?egotter\.com/searches' THEN 'results'
            WHEN referer = '' THEN 'direct'
            ELSE 'others'
          END action,
          count(*) count
        FROM background_search_logs
        WHERE
          created_at BETWEEN :start AND :end
          AND device_type NOT IN ('crawler', 'UNKNOWN')
          #{optional_common_conditions}
          #{optional_background_search_logs_conditions}
        GROUP BY
          session_id, action
      ) b ON (a.session_id = b.session_id)
      GROUP BY
        action
        SQL
      end

      %i(action device_type channel).each do |type|
        define_method("fetch_search_uu_per_#{type}") do
          result = exec_sql(BackgroundSearchLog, send("search_uu_per_#{type}_sql"))
          result.map { |r| r.send(type) }.reject { |a| a == 'NULL' }.uniq.sort.map do |legend|
            {
              name: legend,
              data: result.select { |r| r.send(type) == legend }.map { |r| [to_msec_unixtime(r.date), r.total] },
              visible: !legend.in?(%w(others))
            }
          end
        end

        define_method("fetch_search_num_per_#{type}") do
          result = exec_sql(BackgroundSearchLog, send("search_num_per_#{type}_sql"))
          result.map { |r| r.send(type) }.reject { |a| a == 'NULL' }.uniq.sort.map do |legend|
            {
              name: legend,
              data: result.select { |r| r.send(type) == legend }.map { |r| [to_msec_unixtime(r.date), r.total] },
              visible: !legend.in?(%w(others))
            }
          end
        end

        define_method("fetch_search_num_per_uu_per_#{type}") do
          result = exec_sql(BackgroundSearchLog, send("search_num_per_uu_per_#{type}_sql"))
          result.map { |r| r.send(type) }.reject { |a| a == 'NULL' }.uniq.sort.map do |legend|
            {
              name: legend,
              data: result.select { |r| r.send(type) == legend }.map { |r| [to_msec_unixtime(r.date), r.rate.to_f] },
              visible: !legend.in?(%w(others))
            }
          end
        end
      end

      %i(device_type channel).each do |type|
        define_method("search_uu_per_#{type}_sql") do
          <<-"SQL".strip_heredoc
          SELECT
            :label date,
            #{type == :channel ? "if(channel = '', 'NULL', channel) channel" : type},
            count(DISTINCT session_id) total
          FROM background_search_logs
          WHERE
            created_at BETWEEN :start AND :end
            AND device_type NOT IN ('crawler', 'UNKNOWN')
            #{optional_common_conditions}
            #{optional_background_search_logs_conditions}
          GROUP BY
            #{type}
          SQL
        end

        define_method("search_num_per_#{type}_sql") do
          <<-"SQL".strip_heredoc
          SELECT
            :label date,
            #{type == :channel ? "if(channel = '', 'NULL', channel) channel" : type},
            count(*) total
          FROM background_search_logs
          WHERE
            created_at BETWEEN :start AND :end
            AND device_type NOT IN ('crawler', 'UNKNOWN')
            #{optional_common_conditions}
            #{optional_background_search_logs_conditions}
          GROUP BY
            #{type}
          SQL
        end

        define_method("search_num_per_uu_per_#{type}_sql") do
          <<-"SQL".strip_heredoc
          SELECT
            :label date,
            if(b.session_id IS NULL, 'NULL', b.#{type}) #{type},
            if(count(a.session_id) = 0, 0, sum(b.count) / count(a.session_id)) rate
          FROM (
            SELECT DISTINCT
              session_id
            FROM search_logs
            WHERE
              created_at BETWEEN :start AND :end
              AND device_type NOT IN ('crawler', 'UNKNOWN')
              #{optional_common_conditions}
              #{optional_search_logs_conditions}
          ) a LEFT OUTER JOIN (
            SELECT
              session_id,
              #{type == :channel ? "if(channel = '', 'NULL', channel) channel" : type},
              count(*) count
            FROM background_search_logs
            WHERE
              created_at BETWEEN :start AND :end
              AND device_type NOT IN ('crawler', 'UNKNOWN')
              #{optional_common_conditions}
              #{optional_background_search_logs_conditions}
            GROUP BY
              session_id, #{type}
          ) b ON (a.session_id = b.session_id)
          GROUP BY
            #{type}
          SQL
        end
      end
    end
  end
end
