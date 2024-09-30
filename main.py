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

# Reducing whitespace on the top of the page
st.markdown("""
<style>

.block-container
{
    padding-top: 0.5rem;
    padding-bottom: 0rem;
    margin-top: 0.5rem;
}

</style>
""", unsafe_allow_html=True)

st.title('ClickZetta Lakehouse SQL Dashboard')

TTL = 60

try:
    workspaces = st.secrets.connections.keys()
except:
    workspaces = []
if not workspaces:
    st.warning('No connections found in secrets, please deploy secrets.toml. Example:')
    st.code('''[connections.WORKSPACE]
url = "clickzetta://USER:PASSWORD@INSTANCE.REGION.api.clickzetta.com/WORKSPACE?virtualcluster=VCLUSTER"
''')
    st.stop()
with st.sidebar:
    w = st.query_params.get('workspace', None)
    idx = None
    if w:
        try:
            idx = workspaces.index(w)
        except:
            st.warning(f'Workspace {w} specified in url not found in secrets.')
    workspace = st.selectbox('Workspace', workspaces, index=idx)

if not workspace:
    st.write(':point_left: Fill necessary information in side bar and click analyze button')
    st.stop()

try:
    cz_conn = st.connection(workspace, 'sql', ttl=TTL)
except:
    st.error(f'failed to retrive connection {workspace}.')
    st.info('make sure corresponding connection info is correctly configured in .streamlit/secrets.toml')
    st.code('you can specify workspace in URL, eg. ?workspace=foo')
    st.stop()

filter = "(error_message not like '%Syntax error%' and error_message not like '%table or view not found%' and error_message not like '%cannot resolve column%')"

with st.sidebar:
    with st.form('filter'):
        d = st.date_input('Date', datetime.date.today())
        date_start = d.strftime('%Y-%m-%d 00:00:00')
        date_end = (d + datetime.timedelta(days=1)).strftime('%Y-%m-%d 00:00:00')

        df_vclusters = cz_conn.query('show vclusters;')
        vclusters = df_vclusters['name'].to_list()
        vcluster_selected = st.multiselect('VCluster', vclusters)
        if vcluster_selected:
            tmp = ",".join([f'"{v}"' for v in vcluster_selected])
            filter = f'{filter} and virtual_cluster in ({tmp})'

        df_users = cz_conn.query('show users;')
        users = df_users['name'].to_list()
        user_selected = st.multiselect('User', users)
        if user_selected:
            tmp = ",".join([f'"{v}"' for v in user_selected])
            filter = f'{filter} and job_creator in ({tmp})'

        slow_threshold = st.number_input('Slow SQL Threshold(ms)', value=10000)

        days_of_stat = st.number_input('Days of Stats', value=7)
        date_far = (d + datetime.timedelta(days=-days_of_stat)).strftime('%Y-%m-%d 00:00:00')

        limit = st.number_input('Table Row Limit', value=500)

        ignore_sqls = st.text_input('Ignore SQLs', '', help='use ; to separate multiple SQLs, eg "select 1;show tables"')
        if ignore_sqls:
            ignore_sql_filter = ' and '.join(
                "regexp_replace(regexp_replace(lower(job_text),'^\\\\s*',''),'\\\\s*;\\\\s*$','')!='{}'".
                format(x.strip().lower().replace("'", "\\'")) for x in ignore_sqls.split(';'))
            filter = f'{filter} and ({ignore_sql_filter})'

        errbar = st.selectbox("Execution Time Chart Errbar Series",
                              ["medium, p75, p90", "p90, p95, p99", "min, medium, max", "min, avg, max"])
        errbar_y, errbar_p, errbar_y2 = errbar.split(', ')

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
select total, succeed, floor(100*succeed/total,3) as `succeed rate`,
  failed, ceil(100*failed/total,3) as `failed rate`,
  cancelled, ceil(100*cancelled/total,3) as `cancelled rate`,
  slow, ceil(100*slow/total,3) as `slow rate`,
  first_sql, last_sql
from t
'''
    # st.code(sql)
    df_stats = cz_conn.query(sql, ttl=TTL)
    # st.dataframe(df_stats, hide_index=True)
    st.dataframe(df_stats, use_container_width=True, hide_index=True,
                 column_config={'first_sql': gen_dt_col_conf_with_tz('first sql'),
                                'last_sql': gen_dt_col_conf_with_tz('last sql')})

    st.subheader('Execution Time Distribution Chart')
    sql = f'''
WITH t1 AS (
select cast(execution_time * 1000 as bigint) as execution_time
from information_schema.job_history
where status='SUCCEED' and {filter}
), t2 AS (
select if(execution_time<1,1,execution_time) as execution_time, NTILE(1000) OVER (ORDER BY execution_time asc) AS ntile
from t1
)
SELECT
ntile/10 as percent,
AVG(execution_time) as avg_exec_time,
MAX(execution_time) AS max_exec_time
FROM t2
GROUP BY percent
ORDER BY percent asc;
'''
    df_oneday = cz_conn.query(sql, ttl=TTL)
    c = alt.layer(
        alt.Chart(df_oneday).mark_line().encode(
            x=alt.X('percent', title='percent(%)'),
            y=alt.Y('max_exec_time', title='execution time(ms)').scale(type='log'))
    ).interactive()
    st.altair_chart(c, use_container_width=True)

    # execution time chart
    sql = f'''
with t1 as (
select date_trunc('MINUTE',start_time) as time_minute, execution_time*1000 as execution_time
from information_schema.job_history
where status='SUCCEED' and {filter} )
select time_minute,
  min(execution_time) as min,
  avg(execution_time) as avg,
  percentile(execution_time, 0.50) as medium,
  percentile(execution_time, 0.75) as p75,
  percentile(execution_time, 0.90) as p90,
  percentile(execution_time, 0.95) as p95,
  percentile(execution_time, 0.99) as p99,
  max(execution_time) as max
from t1 group by time_minute order by time_minute asc;
'''
    df_exec_dist = cz_conn.query(sql, ttl=TTL)
    if not df_exec_dist.empty:
        tooltip=[alt.Tooltip('time_minute', title='Time', format='%Y-%m-%d %H:%M'),
                 alt.Tooltip(errbar_y2, title=errbar_y2),
                 alt.Tooltip(errbar_p, title=errbar_p),
                 alt.Tooltip(errbar_y, title=errbar_y)]
        c = alt.layer(
            alt.Chart(df_exec_dist).mark_point(filled=True, size=10).encode(
                y=alt.Y(errbar_p), tooltip=tooltip),
            alt.Chart(df_exec_dist).mark_errorbar().encode(
                y=alt.Y(errbar_y, title=f'execution time(ms): {errbar_y}, {errbar_p}, {errbar_y2}'),
                y2=errbar_y2, tooltip=tooltip, color=alt.value('#e0e0e0'))
        ).encode(
            x=alt.X('time_minute', axis=alt.Axis(format='%Y-%m-%d %H:%M'), title='start time in minute')
        ).interactive(bind_y=False)
        st.altair_chart(c, use_container_width=True)

    # qps & qpm chart
    col1, col2 = st.columns(2)
    col1.subheader('QPM Distribution Chart')
    qpm_header = col2.empty()
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

    # qps & qpm
    c = alt.layer(
        # stack bar
        alt.Chart(df_qps).transform_fold(
            ['max_qps', 'delta'],
            as_=['query', 'value'],
        ).mark_bar(size=1).encode(
            x=alt.X('time_minute:T', axis=alt.Axis(format='%Y-%m-%d %H:%M'), title='start time in minute'),
            y=alt.Y('value', type='quantitative', title=None),
            color=alt.Color('query:N').legend(None),
            order=alt.Order('query:N', sort='descending'),
            tooltip=[alt.Tooltip('time_minute', title='Time', format='%Y-%m-%d %H:%M'),
                        alt.Tooltip('qpm', title='QPM'),
                        alt.Tooltip('max_qps', title='Max QPS in minute')]
        )
    ).interactive(bind_y=False)
    st.altair_chart(c, use_container_width=True)
    qpm_header.code(f'Max QPM: {df_qps["qpm"].max()}')

    col1, col2 = st.columns(2)
    col1.subheader('QPS Distribution Chart (Max QPS in minute)')
    qps_header = col2.empty()
    # qps
    c = alt.layer(
        alt.Chart(df_qps).mark_bar(size=1).encode(
            x=alt.X('time_minute:T', axis=alt.Axis(format='%Y-%m-%d %H:%M'), title='start time in minute'),
            y=alt.Y('max_qps:Q', title=None),
            tooltip=[alt.Tooltip('time_minute', title='Time', format='%Y-%m-%d %H:%M'),
                        alt.Tooltip('max_qps', title='Max QPS in minute')]
        )
    ).interactive(bind_y=False)
    st.altair_chart(c, use_container_width=True)
    qps_header.code(f'Max QPS: {df_qps["max_qps"].max()}')

    # failed sql table
    sql = f'''
select job_id,start_time,execution_time*1000 as execution_time,status,virtual_cluster,job_creator,substring(md5(job_text),1,7) as job_md5,error_message,job_text
from information_schema.job_history
where status='FAILED' and {filter} order by start_time desc
limit {limit}
'''
    df_failed = cz_conn.query(sql, ttl=TTL)
    if not df_failed.empty:
        st.subheader(f'Suspicious failed SQLs ({len(df_failed)})', help='do no include "Syntax error", "table or view not found", "can not resolve column"')
        st.dataframe(df_failed, use_container_width=True, hide_index=True,
                     column_config={'start_time': gen_dt_col_conf_with_tz('start time')})

    # cancelled sql table
    sql = f'''
select job_id, start_time, execution_time*1000 as execution_time, status, virtual_cluster, job_creator, substring(md5(job_text),1,7) as job_md5,job_text
from information_schema.job_history
where status="CANCELLED" and {filter} order by start_time desc
limit {limit}
'''
    df_cancelled = cz_conn.query(sql, ttl=TTL)
    if not df_cancelled.empty:
        st.subheader(f'Cancelled SQLs ({len(df_cancelled)})')
        st.dataframe(df_cancelled, use_container_width=True, hide_index=True,
                     column_config={'start_time': gen_dt_col_conf_with_tz('start time')})

    # slow sql table
    sql = f'''
select job_id, start_time, execution_time*1000 as execution_time, input_bytes, cache_hit, virtual_cluster, job_creator, substring(md5(job_text),1,7) as job_md5,job_text
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

    # multiple days stats
    st.header(f'{days_of_stat} Days Stats [{date_far}, {date_end})')
    sql = f'''
with t1 as (
  select date_format(start_time,'yyyy-MM-dd E') as ds,
    if(status='SUCCEED',1,0) as succeed,
    if(status='FAILED',1,0) as failed,
    if(status='CANCELLED',1,0) as cancelled,
    if(status='SUCCEED' and execution_time*1000>={slow_threshold},1,0) as slow,
    execution_time*1000 as execution_time
  from information_schema.job_history
  where {filter_7days} ),
  t2 as (
    select ds,count(ds) as total,
      sum(succeed) as succeed,
      sum(failed) as failed,
      sum(cancelled) as cancelled,
      sum(slow) as slow,
      avg(execution_time) as avg,
      percentile(execution_time, 0.50) as p50,
      percentile(execution_time, 0.75) as p75,
      percentile(execution_time, 0.90) as p90,
      percentile(execution_time, 0.95) as p95,
      percentile(execution_time, 0.99) as p99,
      max(execution_time) as max,
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
