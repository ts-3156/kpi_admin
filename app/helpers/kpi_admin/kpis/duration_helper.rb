module KpiAdmin
  module Kpis
    module DurationHelper
      def to_msec_unixtime(str)
        (str.in_time_zone('UTC').to_i + utc_offset).in_milliseconds
      end

      def utc_offset
        Time.zone.now.in_time_zone(city).utc_offset
      end

      def city
        case time_zone
          when 'utc' then 'UTC'
          when 'jst' then 'Tokyo'
          else raise "Invalid time_zone: #{time_zone}"
        end
      end

      def time_zone
        %w(utc jst).include?(params[:time_zone]) ? params[:time_zone] : 'utc'
      end

      def frequency
        %w(hourly daily weekly monthly).include?(params[:frequency]) ? params[:frequency] : 'daily'
      end

      def now
        time_zone == 'utc' ? Time.zone.now : Time.zone.now.in_time_zone('Tokyo')
      end

      def duration
        %w(past_14_days past_30_days past_90_days).include?(params[:duration]) ? params[:duration] : 'past_14_days'
      end

      def date_start
        date_array.first.first
      end

      def date_end
        date_array.last.last
      end

      def date_array
        apply_frequency(send(duration))
      end

      def sequence_number
        params[:sequence_number] ? params[:sequence_number].to_i : nil
      end

      def next_sequence_number
        return nil if sequence_number.nil?
        sequence_number < max_sequence_number ? sequence_number + 1 : nil
      end

      def max_sequence_number
        duration.match(/^past_(\d+)_days/)[1].to_i - 1
      end

      def user_id_value
        params[:user_id] && params[:user_id] != '' ? params[:user_id] == 'true' : nil
      end

      def ego_surfing_value
        params[:ego_surfing] && params[:ego_surfing] != '' ? params[:ego_surfing] == 'true' : nil
      end

      def status_value
        params[:status] && params[:status] != '' ? params[:status] == 'true' : nil
      end

      def auto_value
        params[:auto] && params[:auto] != '' ? params[:auto] == 'true' : nil
      end

      def action_values
        params[:_action] && params[:_action] != '' ? Array.wrap("'#{params[:_action]}'") : nil
      end

      def device_type_values
        params[:device_type] && params[:device_type] != '' ? Array.wrap("'#{params[:device_type]}'") : nil
      end

      def channel_value
        params[:channel] && params[:channel] != '' ? params[:channel] : nil
      end

      def context_values
        params[:context] && params[:context] != '' ? Array.wrap("'#{params[:context]}'") : nil
      end

      def name_values
        params[:name] && params[:name] != '' ? Array.wrap("'#{params[:name]}'") : nil
      end

      private

      def apply_frequency(days)
        num =
          case frequency
            when 'hourly' then raise NotImplementedError
            when 'daily' then 1
            when 'weekly' then 7
            when 'monthly' then 30
            else raise "Invalid frequency: #{frequency}"
          end
        days.map { |day| past_n_days(num, day) }
      end

      def past_14_days
        past_n_days(14)
      end

      def past_30_days
        past_n_days(30)
      end

      def past_90_days
        past_n_days(90)
      end

      def past_n_days(num, now = nil)
        now = now() if now.nil?
        start = (now - (num - 1).days).beginning_of_day
        num.times.to_a.map { |i| start + i.days }
      end
    end
  end
end
