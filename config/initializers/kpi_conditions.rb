require 'groupdate'

module KpiAdmin
  module KpiConditions
    def kpi_conditions(context)
     where.not(device_type: %w(crawler UNKNOWN)).
     where(context.optional_common_conditions).
     where(context.send("optional_#{table_name.remove(/^tmp_/)}_conditions")).
     group_by_day(:created_at, range: context.first_datetime..context.last_datetime)
    end
  end
end

class TmpSearchLog < ActiveRecord::Base; end
class TmpBackgroundSearchLog < ActiveRecord::Base; end

[TmpSearchLog, TmpBackgroundSearchLog].each do |table|
  table.extend KpiAdmin::KpiConditions
end
