module KpiAdmin
  module Kpis
    module PvUuHelper

      def fetch_uu
        date = to_msec_unixtime(datetime_label)
        [
          {name: :total, data: [[date, TmpSearchLog.kpi_conditions(view_context).count('DISTINCT session_id').values[0]]]},
          {name: :guest, data: [[date, TmpSearchLog.kpi_conditions(view_context).count('DISTINCT if(user_id  = -1, session_id, NULL)').values[0]]]},
          {name: :login, data: [[date, TmpSearchLog.kpi_conditions(view_context).count('DISTINCT if(user_id != -1, session_id, NULL)').values[0]]]},
        ]
      end

      def fetch_pv
        date = to_msec_unixtime(datetime_label)
        [
          {name: :total, data: [[date, TmpSearchLog.kpi_conditions(view_context).count.values[0]]]},
          {name: :guest, data: [[date, TmpSearchLog.kpi_conditions(view_context).count('if(user_id  = -1, session_id, NULL)').values[0]]]},
          {name: :login, data: [[date, TmpSearchLog.kpi_conditions(view_context).count('if(user_id != -1, session_id, NULL)').values[0]]]},
        ]
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
          date = to_msec_unixtime(datetime_label)
          values = TmpSearchLog.where(created_at: first_datetime..last_datetime).select(type).uniq.pluck(type).reject { |a| a == 'NULL' }
          values.map do |value|
            {
              name: value,
              data: [[date, TmpSearchLog.where(type => value).kpi_conditions(view_context).count('DISTINCT session_id').values[0]]],
              visible: is_visible.call(value)
            }
          end.sort_by { |obj| -obj[:data][0][1] }
        end

        define_method("fetch_pv_per_#{type}") do
          date = to_msec_unixtime(datetime_label)
          values = TmpSearchLog.where(created_at: first_datetime..last_datetime).select(type).uniq.pluck(type).reject { |a| a == 'NULL' }
          values.map do |value|
            {
              name: value,
              data: [[date, TmpSearchLog.where(type => value).kpi_conditions(view_context).count.values[0]]],
              visible: is_visible.call(value)
            }
          end.sort_by { |obj| -obj[:data][0][1] }
        end
      end
    end
  end
end
