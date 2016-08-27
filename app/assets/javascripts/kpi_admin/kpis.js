window.kpis = {};

window.kpis.dateTimeLabelFormats = {
  millisecond: '%H:%M:%S.%L',
  second: '%H:%M:%S',
  minute: '%H:%M',
  hour: '%H:%M',
  day: '%m/%d',
  week: '%m/%d',
  month: '%b \'%y',
  year: '%Y'
};

window.kpis.config = {
  credits: {
    enabled: false
  },
  chart: {
    type: 'line'
  },
  title: {
    text: 'title'
  },
  xAxis: {
    type: 'datetime',
    dateTimeLabelFormats: window.kpis.dateTimeLabelFormats
  },
  yAxis: {
    title: null
  },
  tooltip: {
    valueSuffix: ''
  },
  series: null
};

window.kpis.config_stacked = {
  credits: {
    enabled: false
  },
  chart: {
    type: 'area'
  },
  title: {
    text: 'title'
  },
  xAxis: {
    type: 'datetime',
    dateTimeLabelFormats: window.kpis.dateTimeLabelFormats
  },
  yAxis: {
    title: null
  },
  tooltip: {
    valueSuffix: ''
  },
  plotOptions: {
    area: {
      stacking: 'normal'
    }
  },
  series: null
};

function failed(xhr) {
  console.log(xhr.responseText);
}

function params(type, sequence_number) {
  return {
    type: type,
    time_zone: $('input[name=time_zone]:checked').val(),
    frequency: $('input[name=frequency]:checked').val(),
    duration: $('input[name=duration]:checked').val(),
    sequence_number: sequence_number,
    user_id: $('input[name=user_id]:checked').val(),
    ego_surfing: $('input[name=ego_surfing]:checked').val(),
    _action: $('input[name=_action]:checked').val(),
    status: $('input[name=status]:checked').val(),
    auto: $('input[name=auto]:checked').val(),
    device_type: $('input[name=device_type]:checked').val(),
    channel: $('input[name=channel]:checked').val(),
    context: $('input[name=context]:checked').val(),
    name: $('input[name=name]:checked').val()
  }
}

function fetch(url, type, stacked) {
  var defer = fetch_one_day(url, type, 0);
  var days_count = $('input[name=duration]:checked').data('num') + 1;
  for (var i = 1; i < days_count; i++) {
    defer = defer.then(function (res) {
      stacked ? done_stacked(res) : done(res);
      return fetch_one_day(url, type, res.next_sequence_number)
    }, failed);
  }
  return defer.promise()
}

function fetch_one_day(url, type, sequence_number) {
  return $.get(url, params(type, sequence_number))
}

var charts = {};

function draw(res, config) {
  if (charts[res.type]) {
    var chart = charts[res.type];
    $.each(res[res.type], function (_, new_serie) {
      var found = false;
      $.each(chart.series, function (_, cur_serie) {
        if (new_serie.name == cur_serie.name) {
          cur_serie.addPoint(new_serie.data[0], false);
          found = true;
          return false
        }
      });
      if (!found) {
        chart.addSeries(new_serie, false);
      }
    });
    chart.redraw();
  } else {
    var conf = $.extend(true, {}, config);
    conf.title.text = res.type;
    conf.series = res[res.type];
    conf.chart.renderTo = $('.' + res.type)[0];
    $('.' + res.type).prev().hide();
    charts[res.type] = new Highcharts.Chart(conf);
  }
}

function update_clock(res) {
  $('.date_start').text(res.date_start);
  $('.date_end').text(res.date_end);
  $('.now').text(res.now);
}

function done(res) {
  draw(res, window.kpis.config);
  update_clock(res);
}

function done_stacked(res) {
  draw(res, window.kpis.config_stacked);
  update_clock(res);
}

function after_load() {
  $('input[name=time_zone]:radio').on('change', reload);
  $('input[name=frequency]:radio').on('change', reload);
  $('input[name=duration]:radio').on('change', reload);
  $('input[name=user_id]:radio').on('change', reload);
  $('input[name=ego_surfing]:radio').on('change', reload);
  $('input[name=_action]:radio').on('change', reload);
  $('input[name=status]:radio').on('change', reload);
  $('input[name=auto]:radio').on('change', reload);
  $('input[name=device_type]:radio').on('change', reload);
  $('input[name=channel]:radio').on('change', reload);
  $('input[name=context]:radio').on('change', reload);
  $('input[name=name]:radio').on('change', reload);
}

function before_reload() {
  $('.loading').show();
  $('.kpis').empty();
  $.each(charts, function (_, chart) {
    chart.destroy();
  });
  charts = {};
}