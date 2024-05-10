import datetime
import pandas as pd
import numpy as np
import streamlit as st
from st_aggrid import AgGrid, ColumnsAutoSizeMode, GridUpdateMode, GridOptionsBuilder, ExcelExportMode
import altair as alt
from PIL import Image

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

filter = 'true'
filter_7days = 'true'

with st.form('filter'):

    col1, col2, col3, col4, col5 = st.columns(5)

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

    submitted = st.form_submit_button('Analyze')

if submitted:
    filter_7days = f"{filter} and start_time>='{date_far}'::timestamp and start_time<'{date_end}'::timestamp"
    filter = f"{filter} and start_time>='{date_start}'::timestamp and start_time<'{date_end}'::timestamp"

    st.header(f'24 Hours Stats [{date_start}, {date_end})')
    sql=f'''
with t as (
select count(1) as total,
  sum(if(status='SUCCEED',1,0)) as succeed,
  sum(if(status='RUNNING',1,0)) as running,
  sum(if(status='FAILED',1,0)) as failed,
  sum(if(status='CANCELLED',1,0)) as cancelled,
  min(start_time) as first_sql, max(start_time) as last_sql
from information_schema.job_history
where {filter} )
select total, succeed, round(100*succeed/total,3) as succeed_rate,
  failed, ceil(100*failed/total,3) as failed_rate,
  cancelled, ceil(100*cancelled/total,3) as cancelled_rate,
  running, first_sql, last_sql
from t
'''
    # st.code(sql)
    df_stats = cz_conn.query(sql, ttl=TTL)
    # st.dataframe(df_stats, hide_index=True)
    AgGrid(df_stats,
           use_container_width=True, columns_auto_size_mode=ColumnsAutoSizeMode.FIT_ALL_COLUMNS_TO_VIEW,
           excel_export_mode=ExcelExportMode.TRIGGER_DOWNLOAD,
           enable_enterprise_modules=True, update_mode=GridUpdateMode.SELECTION_CHANGED, reload_data=True)

    sql=f'''
select job_id,start_time,job_creator,job_text,cru,input_bytes,output_bytes
from information_schema.job_history
where status="RUNNING" and {filter};
'''
    df_running = cz_conn.query(sql, ttl=TTL)
    if not df_running.empty:
        st.subheader('Running SQLs')
        # st.dataframe(df_running)
        AgGrid(df_running,
               use_container_width=True, columns_auto_size_mode=ColumnsAutoSizeMode.FIT_ALL_COLUMNS_TO_VIEW,
               excel_export_mode=ExcelExportMode.TRIGGER_DOWNLOAD,
               enable_enterprise_modules=True, update_mode=GridUpdateMode.SELECTION_CHANGED, reload_data=True)

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

    st.subheader('QPS Distribution Chart')
    # QPS
    sql = f'''
WITH t1 as (select date_trunc('SECOND', start_time) as time_second
from information_schema.job_history
where {filter} ),
t2 as ( select time_second, count(1) as qps from t1
group by time_second ),
t3 as ( select date_trunc('MINUTE', time_second) as time_minute, qps
from t2 )
select time_minute, max(qps) as max_qps
from t3
group by time_minute order by time_minute asc;
'''
    # QPM
#     sql = f'''
# WITH t1 as (select date_trunc('MINUTE', start_time) as time_minute
# from information_schema.job_history
# where {filter} )
# select time_minute, count(1) as qpm from t1
# group by time_minute order by time_minute asc;
# '''
    df_qps = cz_conn.query(sql, ttl=TTL)
    c = alt.layer(
        alt.Chart(df_qps).mark_bar(size=1).encode(
            x=alt.X('time_minute', title='time(minute)'),
            y=alt.Y('max_qps', title='qps(max)'))
    ).interactive()
    st.altair_chart(c, use_container_width=True)

    sql = f'''
select job_id, start_time, execution_time*1000 as duration, status, virtual_cluster, job_creator, job_text, error_message
from information_schema.job_history
where status="FAILED" and {filter} order by start_time desc;
'''
    df_failed = cz_conn.query(sql, ttl=TTL)
    if not df_failed.empty:
        st.subheader(f'Failed SQLs ({len(df_failed)})')
        AgGrid(df_failed,
               use_container_width=True, columns_auto_size_mode=ColumnsAutoSizeMode.FIT_ALL_COLUMNS_TO_VIEW,
               excel_export_mode=ExcelExportMode.TRIGGER_DOWNLOAD,
               enable_enterprise_modules=True, update_mode=GridUpdateMode.SELECTION_CHANGED, reload_data=True)

    sql = f'''
select job_id, start_time, execution_time*1000 as duration, status, virtual_cluster, job_creator, job_text, error_message
from information_schema.job_history
where status="CANCELLED" and {filter} order by start_time desc;
'''
    df_cancelled = cz_conn.query(sql, ttl=TTL)
    if not df_cancelled.empty:
        st.subheader(f'Cancelled SQLs ({len(df_cancelled)})')
        AgGrid(df_cancelled,
               use_container_width=True, columns_auto_size_mode=ColumnsAutoSizeMode.FIT_ALL_COLUMNS_TO_VIEW,
               excel_export_mode=ExcelExportMode.TRIGGER_DOWNLOAD,
               enable_enterprise_modules=True, update_mode=GridUpdateMode.SELECTION_CHANGED, reload_data=True)

    sql = f'''
select job_id, start_time, execution_time*1000 as duration, input_bytes, cache_hit, virtual_cluster, job_creator, job_text
from information_schema.job_history
where status="SUCCEED" and execution_time*1000>={slow_threshold} and {filter}
order by start_time desc
'''
    df_slow = cz_conn.query(sql, ttl=TTL)
    if not df_slow.empty:
        st.subheader(f'Slow Succeed SQLs ({len(df_slow)})')
        AgGrid(df_slow,
               use_container_width=True, columns_auto_size_mode=ColumnsAutoSizeMode.FIT_ALL_COLUMNS_TO_VIEW,
               excel_export_mode=ExcelExportMode.TRIGGER_DOWNLOAD,
               enable_enterprise_modules=True, update_mode=GridUpdateMode.SELECTION_CHANGED, reload_data=True)

    st.header(f'{days_of_stat} Days Stats [{date_far}, {date_end})')
    sql = f'''
with t1 as (
  select date_format(start_time,'yyyy-MM-dd E') as ds,
    if(status='SUCCEED',1,0) as succeed,
    if(status='FAILED',1,0) as failed,
    if(status='CANCELLED',1,0) as cancelled,
    execution_time*1000 as duration
  from information_schema.job_history
  where {filter_7days} ),
  t2 as (
    select ds,count(ds) as total,
      sum(succeed) as succeed,
      sum(failed) as failed,
      sum(cancelled) as cancelled,
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
  round(100*succeed/total,3) as succeed_rate,
  ceil(100*failed/total,3) as failed_rate,
  ceil(100*cancelled/total,3) as cancelled_rate,
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
    AgGrid(df_7days,
           use_container_width=True, columns_auto_size_mode=ColumnsAutoSizeMode.FIT_ALL_COLUMNS_TO_VIEW,
           excel_export_mode=ExcelExportMode.TRIGGER_DOWNLOAD,
           enable_enterprise_modules=True, update_mode=GridUpdateMode.SELECTION_CHANGED, reload_data=True)
