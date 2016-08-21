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
      FROM search_logs
      WHERE
        created_at BETWEEN :start AND :end
        AND device_type NOT IN ('crawler', 'UNKNOWN')
        #{optional_common_conditions}
        #{optional_search_logs_conditions}
        SQL
      end

      def fetch_uu_per_referer
        result = exec_sql(SearchLog, uu_per_referer_sql)
        result.map(&:referer).reject { |r| r == 'NULL' }.uniq.sort.map do |legend|
          {
            name: legend,
            data: result.select { |r| r.referer == legend }.map { |r| [to_msec_unixtime(r.date), r.total] },
            visible: !legend.in?(%w(EGOTTER))
          }
        end
      end

      def uu_per_referer_sql
        <<-"SQL".strip_heredoc
        SELECT
          :label date,
          case
            when a._referer like '%egotter%' then 'EGOTTER'
            when a._referer like '%google%' then 'GOOGLE'
            when a._referer like '%yahoo%' then 'YAHOO'
            when a._referer like '%naver%' then 'NAVER'
            when a._referer regexp '(mobile\.)?twitter\.com|t\.co' then 'TWITTER'
            else a._referer
          end referer,
          count(DISTINCT a.session_id) total
        FROM (
          SELECT DISTINCT
            if(referer = '', 'NULL',
              SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(referer, '/', 3), '://', -1), '/', 1), '?', 1)
            ) _referer,
            session_id
          FROM search_logs
          WHERE
            created_at BETWEEN :start AND :end
            AND device_type NOT IN ('crawler', 'UNKNOWN')
            #{optional_common_conditions}
            #{optional_search_logs_conditions}
        ) a
        GROUP BY
          referer
        SQL
      end

      def fetch_pv_per_referer
        result = exec_sql(SearchLog, pv_per_referer_sql)
        result.map(&:referer).reject { |r| r == 'NULL' }.uniq.sort.map do |legend|
          {
            name: legend,
            data: result.select { |r| r.referer == legend }.map { |r| [to_msec_unixtime(r.date), r.total] },
            visible: !legend.in?(%w(EGOTTER))
          }
        end
      end

      def pv_per_referer_sql
        <<-"SQL".strip_heredoc
        SELECT
          :label date,
          case
            when a._referer like '%egotter%' then 'EGOTTER'
            when a._referer like '%google%' then 'GOOGLE'
            when a._referer like '%yahoo%' then 'YAHOO'
            when a._referer like '%naver%' then 'NAVER'
            when a._referer regexp '(mobile\.)?twitter\.com|t\.co' then 'TWITTER'
            else a._referer
          end referer,
          count(*) total
        FROM (
          SELECT
            if(referer = '', 'NULL',
              SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(referer, '/', 3), '://', -1), '/', 1), '?', 1)
            ) _referer,
            session_id
          FROM search_logs
          WHERE
            created_at BETWEEN :start AND :end
            AND device_type NOT IN ('crawler', 'UNKNOWN')
            #{optional_common_conditions}
            #{optional_search_logs_conditions}
        ) a
        GROUP BY
          referer
        SQL
      end

      %i(action device_type channel).each do |type|
        is_visible = -> legend do
          case type
            when :action then %w(new removing removed).include?(legend)
            when :referer then !%w(others egotter).include?(legend)
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
              #{type == :channel ? "if(channel = '', 'NULL', channel) channel" : type},
              count(DISTINCT session_id) total
            FROM search_logs
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
              #{type == :channel ? "if(channel = '', 'NULL', channel) channel" : type},
              count(*) total
            FROM search_logs
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

      def fetch_sign_in
        result = exec_sql(SignInLog, sign_in_sql)
        %i(NewUser ReturningUser).map do |legend|
          {
            name: legend,
            data: result.map { |r| [to_msec_unixtime(r.date), r.send(legend)] }
          }
        end
      end

      def sign_in_sql
        <<-"SQL".strip_heredoc
      SELECT
        :label date,
        count(if(context = 'create', 1, NULL)) 'NewUser',
        count(if(context = 'update', 1, NULL)) 'ReturningUser'
      FROM sign_in_logs
      WHERE
        created_at BETWEEN :start AND :end
        SQL
      end
    end
  end
end
