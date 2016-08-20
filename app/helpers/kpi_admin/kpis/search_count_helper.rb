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

      def fetch_search_rate
        result = exec_sql(BackgroundSearchLog, search_rate_sql)
        %i(guest login).uniq.sort.map do |legend|
          {
            name: legend,
            data: result.map { |r| [to_msec_unixtime(r.date), r.send(legend).to_f] }
          }
        end
      end

      def search_rate_sql
        <<-"SQL".strip_heredoc
      SELECT
        :label date,
        if(count(a.session_id) = 0, 0, sum(b.guest) / sum(a.guest)) guest,
        if(count(a.session_id) = 0, 0, sum(b.login) / sum(a.login)) login
      FROM (
        SELECT
          session_id,
          count(DISTINCT if(user_id = -1, session_id, NULL)) guest,
          count(DISTINCT if(user_id != -1, session_id, NULL)) login
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
          count(if(user_id = -1, 1, NULL)) guest,
          count(if(user_id != -1, 1, NULL)) login
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
        #{optional_common_conditions}
        #{optional_background_search_logs_conditions}
        SQL
      end

      def fetch_search_uu_per_action
        result = exec_sql(BackgroundSearchLog, search_uu_per_action_sql)
        result.map(&:action).map do |legend|
          {
            name: legend,
            data: result.select { |r| r.action == legend }.map { |r| [to_msec_unixtime(r.date), r.total] },
            visible: !legend.in?(%w(others))
          }
        end
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

      def fetch_search_num_per_action
        result = exec_sql(BackgroundSearchLog, search_num_per_action_sql)
        result.map(&:action).uniq.sort.map do |legend|
          {
            name: legend,
            data: result.select { |r| r.action == legend }.map { |r| [to_msec_unixtime(r.date), r.total] },
            visible: !legend.in?(%w(others))
          }
        end
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

      def fetch_search_rate_per_action
        result = exec_sql(BackgroundSearchLog, search_rate_per_action_sql)
        result.map(&:action).reject { |a| a == 'NULL' }.uniq.sort.map do |legend|
          {
            name: legend,
            data: result.select { |r| r.action == legend }.map { |r| [to_msec_unixtime(r.date), r.rate.to_f] },
            visible: !legend.in?(%w(others))
          }
        end
      end

      def search_rate_per_action_sql
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

      def fetch_search_uu_per_device_type
        result = exec_sql(BackgroundSearchLog, search_uu_per_device_type_sql)
        result.map(&:device_type).map do |legend|
          {
            name: legend,
            data: result.select { |r| r.device_type == legend }.map { |r| [to_msec_unixtime(r.date), r.total] }
          }
        end
      end

      def search_uu_per_device_type_sql
        <<-"SQL".strip_heredoc
      SELECT
        :label date,
        device_type,
        count(DISTINCT session_id) total
      FROM background_search_logs
      WHERE
        created_at BETWEEN :start AND :end
        AND device_type NOT IN ('crawler', 'UNKNOWN')
        #{optional_common_conditions}
        #{optional_background_search_logs_conditions}
      GROUP BY device_type
      ORDER BY device_type
        SQL
      end

      def fetch_search_num_per_device_type
        result = exec_sql(BackgroundSearchLog, search_num_per_device_type_sql)
        result.map(&:device_type).map do |legend|
          {
            name: legend,
            data: result.select { |r| r.device_type == legend }.map { |r| [to_msec_unixtime(r.date), r.total] }
          }
        end
      end

      def search_num_per_device_type_sql
        <<-"SQL".strip_heredoc
      SELECT
        :label date,
        device_type,
        count(*) total
      FROM background_search_logs
      WHERE
        created_at BETWEEN :start AND :end
        AND device_type NOT IN ('crawler', 'UNKNOWN')
        #{optional_common_conditions}
        #{optional_background_search_logs_conditions}
      GROUP BY
        device_type
        SQL
      end

      def fetch_search_rate_per_device_type
        result = exec_sql(BackgroundSearchLog, search_rate_per_device_type_sql)
        result.map(&:device_type).reject { |a| a == 'NULL' }.uniq.sort.map do |legend|
          {
            name: legend,
            data: result.select { |r| r.device_type == legend }.map { |r| [to_msec_unixtime(r.date), r.rate.to_f] },
            visible: !legend.in?(%w(others))
          }
        end
      end

      def search_rate_per_device_type_sql
        <<-"SQL".strip_heredoc
      SELECT
        :label date,
        if(b.session_id IS NULL, 'NULL', b.device_type) device_type,
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
          device_type,
          count(*) count
        FROM background_search_logs
        WHERE
          created_at BETWEEN :start AND :end
          AND device_type NOT IN ('crawler', 'UNKNOWN')
          #{optional_common_conditions}
          #{optional_background_search_logs_conditions}
        GROUP BY
          session_id, device_type
      ) b ON (a.session_id = b.session_id)
      GROUP BY
        device_type
        SQL
      end

      def fetch_search_num_per_channel
        result = exec_sql(BackgroundSearchLog, search_num_per_channel_sql)
        result.map { |r| r.channel.to_s }.sort.uniq.map do |legend|
          {
            name: legend,
            data: result.select { |r| r.channel == legend }.map { |r| [to_msec_unixtime(r.date), r.total] },
            visible: !legend.in?(%w(blank))
          }
        end
      end

      def search_num_per_channel_sql
        <<-"SQL".strip_heredoc
      SELECT
        :label date,
        if(channel = '', 'blank', channel) channel,
        count(*) total
      FROM background_search_logs
      WHERE
        created_at BETWEEN :start AND :end
        AND device_type NOT IN ('crawler', 'UNKNOWN')
      GROUP BY channel
      ORDER BY channel
        SQL
      end

      def fetch_search_num_by_google
        result = exec_sql(SearchLog, search_num_by_google_sql)
        %i(not_search search).map do |legend|
          {
            name: legend,
            data: result.map { |r| [to_msec_unixtime(r.date), r.send(legend).to_f] }
          }
        end
      end

      def search_num_by_google_sql
        <<-"SQL".strip_heredoc
      SELECT
        :label date,
        count(if(b.session_id IS NULL, 1, NULL)) not_search,
        count(if(b.session_id IS NOT NULL, 1, NULL)) search
      FROM (
        SELECT session_id
        FROM search_logs
        WHERE
          created_at BETWEEN :start AND :end
          AND device_type NOT IN ('crawler', 'UNKNOWN')
          AND action = 'new'
          AND referer regexp '^https?://(www\.)?google(\.com|\.co\.jp)'
      ) a LEFT OUTER JOIN (
        SELECT session_id
        FROM background_search_logs
        WHERE
          created_at BETWEEN :start AND :end
          AND device_type NOT IN ('crawler', 'UNKNOWN')
          AND referer regexp '^http://(www\.)?egotter\.com/?$'
      ) b ON (a.session_id = b.session_id)
        SQL
      end
    end
  end
end
