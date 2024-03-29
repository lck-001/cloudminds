set hive.exec.dynamic.partition.mode = nonstrict;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.exec.max.dynamic.partitions = 60000;
set mapreduce.job.queuename=root.prod;
with t as (
    SELECT 
        event_type_id,
        event_name,
        event_time,
        model_id,
        tenant_id,
        option,
        cast(operator_id as string) as operator_id,
        cast(project_id as string) as project_id,
        label_type_id,
        -- label_type_name,
        label_id,
        label_name,
        question_id,
        asr_correct_text,
        audio_correct_text,
        submit_time
    from cdmdwd.dwd_cms_svo_asr_label_event_a_d
    where 
        to_date(submit_time) >= to_date('${s_dt_var}')
    and 
        to_date(submit_time) <= to_date('${e_dt_var}')
       
),
--租户补充行业名称
result_t as (
    SELECT 
        o.event_type_id as event_type_id,
        o.event_name as event_name,
        o.event_time as event_time,
        o.model_id as model_id,
        -- o.tenant_id as tenant_id,
        o.option as option,
        o.operator_id as operator_id,
        o.project_id as project_id,
        o.label_type_id as label_type_id,
        -- o.label_type_name as label_type_name,
        o.label_id as label_id,
        o.label_name as label_name,
        o.question_id as question_id,
        o.asr_correct_text as asr_correct_text,
        o.audio_correct_text as audio_correct_text,

        a.sv_agent_id as sv_agent_id,
        a.sv_agent_name as sv_agent_name,
        a.question_text as question_text,
        a.answer_text as answer_text,
        a.sv_answer_text as sv_answer_text,
        a.qa_from as qa_from,
        a.nlu_event_time as nlu_event_time,
        o.submit_time,

        --tenant
        a.tenant__tenant_id,
        a.tenant__tenant_name,
        a.tenant__tenant_type,
        a.tenant__industry_id,
        a.tenant__industry_name,
        a.tenant__sub_sys,
        a.tenant__time_zone,
        a.tenant__net_env_id,
        a.tenant__mcs_area,
        a.tenant__device_num,
        a.tenant__region,
        a.tenant__address,
        a.tenant__contact,
        a.tenant__phone,
        a.tenant__telephone,
        a.tenant__seller,
        a.tenant__email,
        a.tenant__status,
        a.tenant__status_name,
        a.tenant__charge_time,
        a.tenant__expired_time,
        a.tenant__logo,
        a.tenant__description,
        a.tenant__brand,
        a.tenant__is_need_vpn,
        a.tenant__create_time,
        --robot
        a.robot__robot_id,
        a.robot__robot_name,
        a.robot__robot_type_id,
        a.robot__robot_type_inner_name,
        a.robot__robot_type_name,
        a.robot__asset_code,
        a.robot__asset_type,
        a.robot__product_type_code,
        a.robot__product_type_code_name,
        a.robot__product_id,
        a.robot__product_id_name,
        a.robot__assets_status,
        a.robot__assets_status_name,
        a.robot__status,
        a.robot__status_name,
        a.robot__robot_manufacturer_id,
        a.robot__robot_manufacturer_name,
        a.robot__model,
        a.robot__os,
        a.robot__version_code,
        a.robot__software_version,
        a.robot__hardware_version,
        a.robot__head_url,
        a.robot__regist_time,
        a.robot__bind_status,
        a.robot__bind_status_name,
        a.robot__is_hi_service,
        a.robot__is_privacy_enhance,
        a.robot__remind_policy_id,
        a.robot__sku,
        a.robot__quality_date,
        a.robot__customer_quality_date,
        a.robot__out_type_state,
        a.robot__out_type_state_name,
        a.robot__product_date,
        a.robot__in_stock_date,
        a.robot__out_stock_date,
        a.robot__in_stock_staff_id,
        a.robot__in_stock_staff_name,
        a.robot__out_stock_staff_id,
        a.robot__out_stock_staff_name,
        a.robot__robot_note,
        a.robot__description,
        a.robot__create_time,
        -- robot_account
        a.robot_account__robot_account_id,
        a.robot_account__robot_account_name,
        a.robot_account__email,
        a.robot_account__phone,
        a.robot_account__status,
        a.robot_account__status_name,
        a.robot_account__privacy_type,
        a.robot_account__privacy_type_name,
        a.robot_account__is_hi_service,
        a.robot_account__is_privacy_enhance,
        a.robot_account__is_auto_takeover,
        a.robot_account__app_id,
        a.robot_account__video_mode,
        a.robot_account__phone_number_prefix,
        a.robot_account__operator_id,
        a.robot_account__ross_credential_id,
        a.robot_account__create_time,

        a.fqa_cate__cate_name_path,
        a.user_qa_cate__cate_name_path,
        a.rcu_audio,
        a.is_noise,
        a.qa_flag,
        a.asr_vendor,
        a.dialect,
        a.algo,
        a.hi_intelligence_msg,
        a.hi_intelligence_msg_from,
        a.hi_intelligence_msg_from_name,
        a.hi_intelligence_msg_type,
        a.is_hi_intelligence,
        a.hi_qa_msg_from,
        a.hi_qa_msg_send_name,
        a.is_hi_qa,

        a.question_language,
        a.cmd_k8s_env_name,
        a.nlu_k8s_env_name,
        a.intent__intent_id,
        a.intent__intent_name,
        a.domain__domain_id,
        a.domain__domain_name,
        a.asr_domain
    FROM(
        SELECT
            event_type_id,
            event_name,
            event_time,
            model_id,
            tenant_id,
            option,
            operator_id,
            project_id,
            label_type_id,
            -- label_type_name,
            label_id,
            label_name,
            question_id,
            submit_time,
            asr_correct_text,
            audio_correct_text
        from t
    ) o
    left JOIN (
        select 
            *,
            row_number() over (
                partition by t.question_id order by t.nlu_event_time desc
            ) as rnk
        from (
            select 
                sv_agent_id,
                sv_agent_name,
                question_text,
                answer_text,
                sv_answer_text,
                qa_from,
                nlu_event_time,
                --tenant
                tenant__tenant_id,
                tenant__tenant_name,
                tenant__tenant_type,
                tenant__industry_id,
                tenant__industry_name,
                tenant__sub_sys,
                tenant__time_zone,
                tenant__net_env_id,
                tenant__mcs_area,
                tenant__device_num,
                tenant__region,
                tenant__address,
                tenant__contact,
                tenant__phone,
                tenant__telephone,
                tenant__seller,
                tenant__email,
                tenant__status,
                tenant__status_name,
                tenant__charge_time,
                tenant__expired_time,
                tenant__logo,
                tenant__description,
                tenant__brand,
                tenant__is_need_vpn,
                tenant__create_time,
                --robot
                robot__robot_id,
                robot__robot_name,
                robot__robot_type_id,
                robot__robot_type_inner_name,
                robot__robot_type_name,
                robot__asset_code,
                robot__asset_type,
                robot__product_type_code,
                robot__product_type_code_name,
                robot__product_id,
                robot__product_id_name,
                robot__assets_status,
                robot__assets_status_name,
                robot__status,
                robot__status_name,
                robot__robot_manufacturer_id,
                robot__robot_manufacturer_name,
                robot__model,
                robot__os,
                robot__version_code,
                robot__software_version,
                robot__hardware_version,
                robot__head_url,
                robot__regist_time,
                robot__bind_status,
                robot__bind_status_name,
                robot__is_hi_service,
                robot__is_privacy_enhance,
                robot__remind_policy_id,
                robot__sku,
                robot__quality_date,
                robot__customer_quality_date,
                robot__out_type_state,
                robot__out_type_state_name,
                robot__product_date,
                robot__in_stock_date,
                robot__out_stock_date,
                robot__in_stock_staff_id,
                robot__in_stock_staff_name,
                robot__out_stock_staff_id,
                robot__out_stock_staff_name,
                robot__robot_note,
                robot__description,
                robot__create_time,
                -- robot_account
                robot_account__robot_account_id,
                robot_account__robot_account_name,
                robot_account__email,
                robot_account__phone,
                robot_account__status,
                robot_account__status_name,
                robot_account__privacy_type,
                robot_account__privacy_type_name,
                robot_account__is_hi_service,
                robot_account__is_privacy_enhance,
                robot_account__is_auto_takeover,
                robot_account__app_id,
                robot_account__video_mode,
                robot_account__phone_number_prefix,
                robot_account__operator_id,
                robot_account__ross_credential_id,
                robot_account__create_time,
                question_id,

                fqa_cate__cate_name_path,
                user_qa_cate__cate_name_path,
                rcu_audio,
                is_noise,
                qa_flag,
                asr_vendor,
                dialect,
                algo,
                hi_intelligence_msg,
                hi_intelligence_msg_from,
                hi_intelligence_msg_from_name,
                hi_intelligence_msg_type,
                is_hi_intelligence,
                hi_qa_msg_from,
                hi_qa_msg_send_name,
                is_hi_qa,

                question_language,
                cmd_k8s_env_name,
                nlu_k8s_env_name,
                intent__intent_id,
                intent__intent_name,
                domain__domain_id,
                domain__domain_name,
                asr_domain
            from cdmdwm.dwm_cmd_audio_process_i_d
            where 
                to_date(nlu_event_time) <= to_date('${e_dt_var}')
            and
                to_date(nlu_event_time) >= date_sub('${s_dt_var}', 30)
        ) t
    ) as a on o.question_id = a.question_id where a.rnk = 1
)

insert overwrite table cdmdwm.dwm_svo_asr_label_event_i_d partition(dt) 
select 
    *,
    to_date(submit_time) as dt
from result_t;
