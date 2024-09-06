import datetime
import streamlit as st
import altair as alt
from PIL import Image
import tzlocal

icon = None
try:
    icon = Image.open('icon.png')
except:
    pass

st.set_page_config(
    page_title="ClickZetta Lakehouse SQL Monitor",
    page_icon=icon,
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items = {
        'About': 'https://github.com/clickzetta/clickzetta-sql-dashboard'
    }
)

st.title('ClickZetta Lakehouse SQL Dashboard')

TTL = 60

workspace = st.text_input('workspace', st.query_params.get('workspace', None))
if not workspace:
    st.stop()

try:
    cz_conn = st.connection(workspace, 'sql', ttl=TTL)
except:
    st.error(f'failed to retrive connection {workspace}.')
    st.info('make sure corresponding connection info is correctly configured in .streamlit/secrets.toml')
    st.code('you can specify workspace in URL, eg. ?workspace=foo')
    st.stop()

filter = "(error_message not like '%Syntax error%' and error_message not like '%table or view not found%' and error_message not like '%cannot resolve column%')"
filter_7days = "(error_message not like '%Syntax error%' and error_message not like '%table or view not found%' and error_message not like '%cannot resolve column%')"

with st.form('filter'):

    col1, col2, col3, col4, col5, col6 = st.columns(6)

    with col1:
        d = st.date_input('Date', datetime.date.today())
        date_start = d.strftime('%Y-%m-%d 00:00:00')
        date_end = (d + datetime.timedelta(days=1)).strftime('%Y-%m-%d 00:00:00')

    with col2:
        df_vclusters = cz_conn.query('show vclusters;')
        vclusters = df_vclusters['name'].to_list()
        vcluster_selected = st.multiselect('VCluster', vclusters)
        if vcluster_selected:
            tmp = ",".join([f'"{v}"' for v in vcluster_selected])
            filter = f'{filter} and virtual_cluster in ({tmp})'

    with col3:
        df_users = cz_conn.query('show users;')
        users = df_users['name'].to_list()
        user_selected = st.multiselect('User', users)
        if user_selected:
            tmp = ",".join([f'"{v}"' for v in user_selected])
            filter = f'{filter} and job_creator in ({tmp})'
    with col4:
        slow_threshold = st.number_input('slow query(ms)', value=10000)

    with col5:
        days_of_stat = st.number_input('days of stats', value=7)
        date_far = (d + datetime.timedelta(days=-days_of_stat)).strftime('%Y-%m-%d 00:00:00')

    with col6:
        limit = st.number_input('table row limit', value=500)

    submitted = st.form_submit_button('Analyze')

def gen_dt_col_conf_with_tz(title: str) -> st.column_config.DatetimeColumn:
    return st.column_config.DatetimeColumn(title, timezone=tzlocal.get_localzone().key)

if submitted:
    filter_7days = f"{filter} and start_time>='{date_far}'::timestamp and start_time<'{date_end}'::timestamp"
    filter = f"{filter} and start_time>='{date_start}'::timestamp and start_time<'{date_end}'::timestamp"

    st.header(f'24 Hours Stats [{date_start}, {date_end})')
    sql=f'''
with t as (
select count(1) as total,
  sum(if(status='SUCCEED',1,0)) as succeed,
  sum(if(status='FAILED',1,0)) as failed,
  sum(if(status='CANCELLED',1,0)) as cancelled,
  sum(if(status='SUCCEED' and execution_time*1000>={slow_threshold},1,0)) as slow,
  min(start_time) as first_sql, max(start_time) as last_sql
from information_schema.job_history
where {filter} )
select total, succeed, round(100*succeed/total,3) as `succeed rate`,
  failed, ceil(100*failed/total,3) as `failed rate`,
  cancelled, ceil(100*cancelled/total,3) as `cancelled rate`,
  slow, ceil(100*cancelled/total,3) as `slow rate`,
  first_sql, last_sql
from t
'''
    # st.code(sql)
    df_stats = cz_conn.query(sql, ttl=TTL)
    # st.dataframe(df_stats, hide_index=True)
    st.dataframe(df_stats, use_container_width=True, hide_index=True,
                 column_config={'first_sql': gen_dt_col_conf_with_tz('first sql'),
                                'last_sql': gen_dt_col_conf_with_tz('last sql')})

    st.subheader('Duration Distribution Chart')
    sql = f'''
WITH t1 AS (
select cast(execution_time * 1000 as bigint) as duration
from information_schema.job_history
where status='SUCCEED' and {filter}
), t2 AS (
select if(duration<1,1,duration) as duration, NTILE(100) OVER (ORDER BY duration asc) AS percent
from t1
)
SELECT
percent,
AVG(duration) as avg_duration,
MAX(duration) AS max_duration
FROM t2
GROUP BY percent
ORDER BY percent asc;
'''
    df_oneday = cz_conn.query(sql, ttl=TTL)
    c = alt.layer(
        alt.Chart(df_oneday).mark_line(point=True).encode(
            x=alt.X('percent', title='percent(%)'),
            y=alt.Y('max_duration', title='duration(ms)').scale(type='log'))
    ).interactive()
    st.altair_chart(c, use_container_width=True)

    col1, col2 = st.columns(2)
    col1.subheader('Concurrency Distribution Chart')
    max_header = col2.empty()
    # QPS
    sql = f'''
WITH t1 as (select date_trunc('SECOND', start_time) as time_second
from information_schema.job_history
where {filter} ),
t2 as ( select time_second, count(1) as qps from t1
group by time_second ),
t3 as ( select date_trunc('MINUTE', time_second) as time_minute, qps
from t2 )
select time_minute, max(qps) as max_qps, sum(qps) as qpm, (sum(qps) - max(qps)) as delta
from t3
group by time_minute order by time_minute asc;
'''
    df_qps = cz_conn.query(sql, ttl=TTL)
    c = alt.layer(
        # stack bar
        alt.Chart(df_qps).transform_fold(
            ['max_qps', 'delta'],
            as_=['query', 'value'],
        ).mark_bar(size=1).encode(
            x=alt.X('time_minute:T', axis=alt.Axis(format='%Y-%m-%d %H:%M'), title='time in minute'),
            y=alt.Y('value', type='quantitative', title=None),
            color=alt.Color('query:N').legend(None),
            order=alt.Order('query:N', sort='descending'),
            tooltip=[alt.Tooltip('time_minute', title='Time', format='%Y-%m-%d %H:%M'),
                     alt.Tooltip('qpm', title='QPM'),
                     alt.Tooltip('max_qps', title='Max QPS in minute')]
        )
    ).interactive(bind_y=False)
    st.altair_chart(c, use_container_width=True)
    max_header.code(f'Max QPS: {df_qps["max_qps"].max()}, Max QPM: {df_qps["qpm"].max()}')

    sql = f'''
select job_id,start_time,execution_time*1000 as duration,status,virtual_cluster,job_creator,substring(md5(job_text),1,7) as job_md5,error_message,job_text
from information_schema.job_history
where status='FAILED' and {filter} order by start_time desc
limit {limit}
'''
    df_failed = cz_conn.query(sql, ttl=TTL)
    if not df_failed.empty:
        st.subheader(f'Suspicious failed SQLs ({len(df_failed)})', help='do no include "Syntax error", "table or view not found", "can not resolve column"')
        st.dataframe(df_failed, use_container_width=True, hide_index=True,
                     column_config={'start_time': gen_dt_col_conf_with_tz('start time')})

    sql = f'''
select job_id, start_time, execution_time*1000 as duration, status, virtual_cluster, job_creator, substring(md5(job_text),1,7) as job_md5,job_text
from information_schema.job_history
where status="CANCELLED" and {filter} order by start_time desc
limit {limit}
'''
    df_cancelled = cz_conn.query(sql, ttl=TTL)
    if not df_cancelled.empty:
        st.subheader(f'Cancelled SQLs ({len(df_cancelled)})')
        st.dataframe(df_cancelled, use_container_width=True, hide_index=True,
                     column_config={'start_time': gen_dt_col_conf_with_tz('start time')})

    sql = f'''
select job_id, start_time, execution_time*1000 as duration, input_bytes, cache_hit, virtual_cluster, job_creator, substring(md5(job_text),1,7) as job_md5,job_text
from information_schema.job_history
where status="SUCCEED" and execution_time*1000>={slow_threshold} and {filter}
order by start_time desc
limit {limit}
'''
    df_slow = cz_conn.query(sql, ttl=TTL)
    if not df_slow.empty:
        st.subheader(f'Slow Succeed SQLs ({len(df_slow)})')
        st.dataframe(df_slow, use_container_width=True, hide_index=True,
                     column_config={'start_time': gen_dt_col_conf_with_tz('start time')})

    st.header(f'{days_of_stat} Days Stats [{date_far}, {date_end})')
    sql = f'''
with t1 as (
  select date_format(start_time,'yyyy-MM-dd E') as ds,
    if(status='SUCCEED',1,0) as succeed,
    if(status='FAILED',1,0) as failed,
    if(status='CANCELLED',1,0) as cancelled,
    if(status='SUCCEED' and execution_time*1000>={slow_threshold},1,0) as slow,
    execution_time*1000 as duration
  from information_schema.job_history
  where {filter_7days} ),
  t2 as (
    select ds,count(ds) as total,
      sum(succeed) as succeed,
      sum(failed) as failed,
      sum(cancelled) as cancelled,
      sum(slow) as slow,
      avg(duration) as avg,
      percentile(duration, 0.50) as p50,
      percentile(duration, 0.75) as p75,
      percentile(duration, 0.90) as p90,
      percentile(duration, 0.95) as p95,
      percentile(duration, 0.99) as p99,
      max(duration) as max,
    from t1
    group by ds
  )
select ds as date,total,
  succeed,
  floor(100*succeed/total,3) as `succeed rate`,
  failed,
  ceil(100*failed/total,3) as `failed rate`,
  cancelled,
  ceil(100*cancelled/total,3) as `cancelled rate`,
  slow,
  ceil(100*slow/total,3) as `slow rate`,
  avg::bigint as avg,
  p50::bigint as p50,
  p75::bigint as p75,
  p90::bigint as p90,
  p95::bigint as p95,
  p99::bigint as p99,
  max::bigint as max
from t2
order by ds desc
'''
    # st.code(sql)
    df_7days = cz_conn.query(sql, ttl=TTL)
    st.dataframe(df_7days, use_container_width=True, hide_index=True)
