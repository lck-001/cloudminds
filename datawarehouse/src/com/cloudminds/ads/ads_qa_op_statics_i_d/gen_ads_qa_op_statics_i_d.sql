set hive.exec.dynamic.partition.mode = nonstrict;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.exec.max.dynamic.partitions = 60000;
set mapreduce.job.queuename=root.users.liuhao;

with qa_op_tmp as (
    select
        concat_ws('###',qa_id,qa_type) as qa,
        concat_ws('###',operator_id,operator) as operator,
        agent_id,
        concat_ws('###',qa_cate_id,cate_id_path,cate_name_path) as qa_cate,
        work_cnt_d,
        dt
    from cdmdws.dws_svo_qa_op_i_d
    where dt >= '${s_dt_var}' and dt <= '${e_dt_var}'
),
-- qa_cube as (
--     select
--         qa,
--         operator,
--         qa_cate,
--         agent_id,
--         dt,
--         sum(work_cnt_d) as work_cnt_d
--     from qa_op_tmp
--     group by qa,operator,qa_cate,agent_id,dt
--     grouping sets ((qa,dt),(operator,dt),(qa_cate,dt),(agent_id,dt),(qa,operator,dt),(qa,qa_cate,dt),(qa,agent_id,dt),(operator,qa_cate,dt),(operator,agent_id,dt),(qa_cate,agent_id,dt),(qa,operator,qa_cate,agent_id,dt))
-- )
qa_cube as (
    select
        split(qa,'###')[0] as qa_id,
        split(qa,'###')[1] as qa_type,
        split(operator,'###')[0] as operator_id,
        split(operator,'###')[1] as operator,
        split(qa_cate,'###')[0] as qa_cate_id,
        split(qa_cate,'###')[1] as cate_id_path,
        split(qa_cate,'###')[2] as cate_name_path,
        agent_id,
        dt,
        work_cnt_d
    from (
        select
            qa,
            operator,
            qa_cate,
            agent_id,
            dt,
            sum(work_cnt_d) as work_cnt_d
        from qa_op_tmp
        group by qa,operator,qa_cate,agent_id,dt
        with cube
    ) t where dt is null
)
insert overwrite table cdmads.ads_qa_op_statics_i_d partition(dt)
    select
        qa_id,
        qa_type,
        operator_id,
        operator,
        agent_id,
        qa_cate_id,
        cate_id_path,
        cate_name_path,
        work_cnt_d,
        dt
    from qa_cube;