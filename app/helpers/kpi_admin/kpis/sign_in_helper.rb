module KpiAdmin
  module Kpis
    module SignInHelper
      def fetch_sign_in_uu
        result = exec_sql(SignInLog, sign_in_uu_sql)
        %i(NewUser ReturningUser).map do |legend|
          {
            name: legend,
            data: result.map { |r| [to_msec_unixtime(r.date), r.send(legend)] }
          }
        end
      end

      def sign_in_uu_sql
        <<-"SQL".strip_heredoc
      SELECT
        :label date,
        count(DISTINCT if(context = 'create', session_id, NULL)) 'NewUser',
        count(DISTINCT if(context = 'update', session_id, NULL)) 'ReturningUser'
      FROM sign_in_logs
      WHERE
        created_at BETWEEN :start AND :end
        SQL
      end

      def fetch_sign_in_num
        result = exec_sql(SignInLog, sign_in_num_sql)
        %i(NewUser ReturningUser).map do |legend|
          {
            name: legend,
            data: result.map { |r| [to_msec_unixtime(r.date), r.send(legend)] }
          }
        end
      end

      def sign_in_num_sql
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

      def fetch_welcome_to_login_uu
        result = exec_sql(SearchLog, welcome_to_login_sql)
        %i(guest welcome login).map do |legend|
          {
            name: legend,
            data: result.map { |r| [to_msec_unixtime(r.date), r.send(legend)] }
          }
        end
      end

      def fetch_welcome_to_login_ctr_cvr
        result = exec_sql(SearchLog, welcome_to_login_sql)
        %i(ctr cvr).map do |legend|
          {
            name: legend,
            data: result.map { |r| [to_msec_unixtime(r.date), r.send(legend).to_f] }
          }
        end
      end

      def welcome_to_login_sql
        <<-"SQL".strip_heredoc
        SELECT
          :label date,
          count(DISTINCT a.session_id) guest,
          count(DISTINCT b.session_id) welcome,
          count(DISTINCT c.session_id) login,
          count(DISTINCT b.session_id) / count(DISTINCT a.session_id) ctr,
          count(DISTINCT c.session_id) / count(DISTINCT b.session_id) cvr
        FROM (
          SELECT DISTINCT session_id
          FROM search_logs
          WHERE
            created_at BETWEEN :start AND :end
            AND device_type NOT IN ('crawler', 'UNKNOWN')
            AND action != 'welcome'
            AND user_id = -1
            #{optional_common_conditions}
            #{optional_search_logs_conditions}
        ) a LEFT OUTER JOIN (
          SELECT DISTINCT session_id
          FROM search_logs
          WHERE
            created_at BETWEEN :start AND :end
            AND device_type NOT IN ('crawler', 'UNKNOWN')
            AND action = 'welcome'
            AND user_id = -1
            #{optional_common_conditions}
            #{optional_search_logs_conditions}
        ) b ON (a.session_id = b.session_id) LEFT OUTER JOIN (
          SELECT DISTINCT session_id
          FROM search_logs
          WHERE
            created_at BETWEEN :start AND :end
            AND device_type NOT IN ('crawler', 'UNKNOWN')
            AND action != 'welcome'
            AND user_id != -1
            #{optional_common_conditions}
            #{optional_search_logs_conditions}
        ) c ON (b.session_id = c.session_id)
        SQL
      end
      alias_method :welcome_to_login_uu_sql, :welcome_to_login_sql
      alias_method :welcome_to_login_ctr_cvr_sql, :welcome_to_login_sql
    end
  end
end
