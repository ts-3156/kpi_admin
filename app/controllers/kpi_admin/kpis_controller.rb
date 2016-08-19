require_dependency "kpi_admin/application_controller"

module KpiAdmin
  class KpisController < ApplicationController
    include KpisHelper
    include Kpis::DurationHelper
    include Kpis::PvUuHelper
    include Kpis::SearchNumHelper

    METHOD_TYPES = Kpis::PvUuHelper.public_instance_methods + Kpis::SearchNumHelper.public_instance_methods

    def index
    end

    %i(one two).each do |name|
      define_method(name) do
        return render unless request.xhr?

        type = METHOD_TYPES.include?("fetch_#{params[:type]}".to_sym) ? params[:type] : 'uu'
        result = {
          type: type,
          type => send("fetch_#{type}"),
          now: now,
          date_start: date_start.beginning_of_day,
          date_end: date_end.end_of_day,
          city: city,
          time_zone: time_zone,
          frequency: frequency,
          duration: duration,
          date_array: date_array,
          sequence_number: sequence_number,
          next_sequence_number: next_sequence_number,
          max_sequence_number: max_sequence_number,
          sql: show_sql(type, date_array.last),
          url: request.path,
        }
        render json: result, status: 200
      end
    end

    def table
      return render unless request.xhr?

      result = {twitter_users: fetch_twitter_users_num}
      if request.referer.end_with?(action_name)
        result.update(
          twitter_users_uid: fetch_twitter_users_uid_num,
          friends: fetch_friends_num,
          followers: fetch_followers_num
        )
      end
      render json: result, status: 200
    end

    def rr
      return render unless request.xhr?

      id_type = %w(user_id uid).include?(params[:id_type]) ? params[:id_type] : :session_id
      date_index_max = [params[:date_index_max].to_i, 9].min
      date_start_index = [params[:date_start_index].to_i, 0].max
      date_end_index = [params[:date_end_index].to_i, date_index_max].min
      y_index_range = date_start_index..date_end_index
      yAxis_categories = (date_index_max + 1).times.map { |i| (now - i.days) }
      xAxis_categories = (date_index_max + 1).times.to_a
      cells = {}

      yAxis_categories.each.with_index do |day, y|
        next unless y_index_range.include?(y)

        ids = SearchLog.except_crawler.where(created_at: day.all_day).select(id_type).uniq.pluck(id_type)
        xAxis_categories.each do |x|
          if ids.empty?
            cells[[x, y]] = 0
            next
          end
          cells[[x, y]] = SearchLog.except_crawler.where(created_at: (day + x.days).all_day, id_type => ids).count("DISTINCT #{id_type}")
        end
      end

      format = params[:format] == 'percentage' ? 'percentage' : 'number'
      if format == 'percentage'
        tmp = {}
        cells.each do |(x, y), cell|
          value = cells[[0, y]].to_i == 0 ? 0.0 : 100.0 * cell / cells[[0, y]]
          tmp[[x, y]] = value.round(1)
        end
        cells = tmp
      end

      result = {
        title: "RR(#{id_type}, #{format})",
        format: format,
        id_type: id_type,
        sequence_number: params[:sequence_number].to_i,
        date_index_max: date_index_max,
        date_start_index: date_start_index,
        date_end_index: date_end_index,
        xAxis_categories: xAxis_categories.dup,
        yAxis_categories: yAxis_categories.map { |d| d.to_date.strftime('%m/%d') },
        cells: cells.map { |(x, y), cell| [x, y, cell] }
      }
      render json: result, status: 200
    end

    private

    def fetch_twitter_users_num
      result = TwitterUser.find_by_sql([twitter_users_num_sql, {start: date_start, end: date_end}])
      %i(guest login).map do |legend|
        {
          name: legend,
          data: result.map { |r| [to_msec_unixtime(r.date), r.send(legend)] }
        }
      end
    end

    def twitter_users_num_sql
      <<-'SQL'.strip_heredoc
      SELECT
        date(created_at) date,
        count(*) total,
        count(if(user_id = -1, -1, NULL)) guest,
        count(if(user_id != -1, user_id, NULL)) login
      FROM twitter_users
      WHERE
        created_at BETWEEN :start AND :end
      GROUP BY date(created_at)
      ORDER BY date(created_at);
      SQL
    end

    def fetch_twitter_users_uid_num
      result = TwitterUser.find_by_sql([twitter_users_uid_num_sql, {start: date_start, end: date_end}])
      %i(total unique_uid).map do |legend|
        {
          name: legend,
          data: result.map { |r| [to_msec_unixtime(r.date), r.send(legend)] }
        }
      end
    end

    def twitter_users_uid_num_sql
      <<-'SQL'.strip_heredoc
      SELECT
        date(created_at) date,
        count(*) total,
        count(DISTINCT uid) unique_uid
      FROM twitter_users
      WHERE
        created_at BETWEEN :start AND :end
      GROUP BY date(created_at)
      ORDER BY date(created_at);
      SQL
    end

    def fetch_friends_num
      result = Friend.find_by_sql([friends_num_sql, {start: date_start, end: date_end}])
      %i(total).map do |legend|
        {
          name: legend,
          data: result.map { |r| [to_msec_unixtime(r.date), r.send(legend)] }
        }
      end
    end

    def friends_num_sql
      <<-'SQL'.strip_heredoc
      SELECT
        date(created_at) date,
        count(*) total
      FROM friends
      WHERE
        created_at BETWEEN :start AND :end
      GROUP BY date(created_at)
      ORDER BY date(created_at);
      SQL
    end

    def fetch_followers_num
      result = Follower.find_by_sql([followers_num_sql, {start: date_start, end: date_end}])
      %i(total).map do |legend|
        {
          name: legend,
          data: result.map { |r| [to_msec_unixtime(r.date), r.send(legend)] }
        }
      end
    end

    def followers_num_sql
      <<-'SQL'.strip_heredoc
      SELECT
        date(created_at) date,
        count(*) total
      FROM followers
      WHERE
        created_at BETWEEN :start AND :end
      GROUP BY date(created_at)
      ORDER BY date(created_at);
      SQL
    end
  end
end
