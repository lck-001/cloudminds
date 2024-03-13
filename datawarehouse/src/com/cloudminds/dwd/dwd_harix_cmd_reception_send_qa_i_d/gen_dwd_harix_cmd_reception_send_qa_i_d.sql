set hive.exec.dynamic.partition.mode = nonstrict;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.exec.max.dynamic.partitions = 60000;
set mapreduce.job.queuename=root.users.liuhao;
set hive.execution.engine=mr;

with reception_send_tmp as (
    select
        json_extract,
        k8s_svc_name,
        k8s_env_name,
        tdate,
        event_time
    from (
        select
            json_extract,
            k8s_svc_name,
            k8s_env_name,
            tdate,
            case when length(GET_JSON_OBJECT(json_extract,'$.json_extract.timestamp')) = 10 then nvl(concat(from_unixtime( cast(GET_JSON_OBJECT(json_extract,'$.json_extract.timestamp') as int),'yyyy-MM-dd HH:mm:ss'),'.000'),'')
                 when length(GET_JSON_OBJECT(json_extract,'$.json_extract.timestamp')) >= 23 then nvl(SUBSTRING(REGEXP_REPLACE(GET_JSON_OBJECT(json_extract,'$.json_extract.timestamp'),'T',' '),0,23),'')
                 else nvl(concat(from_unixtime( cast(GET_JSON_OBJECT(json_extract,'$.json_extract.timestamp')/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(GET_JSON_OBJECT(json_extract,'$.json_extract.timestamp'),11,3)),'')
            end as event_time
        from hari.harix_etl_es
        where tdate >= date_sub('${s_dt_var}',2) and tdate <= '${e_dt_var}'
        and trim(GET_JSON_OBJECT(json_extract,'$.json_extract.rodType')) = 'RobotMindSendQa'
        and GET_JSON_OBJECT(json_extract,'$.json_extract.robotData.request.Answer') is not null
        and trim(k8s_svc_name) in ('harix-skill-robot')
    ) t where substring(event_time,0,10) >= date_sub('${s_dt_var}',2) and substring(event_time,0,10) <= '${e_dt_var}'
),
reception_send as (
    select
        case when nvl(GET_JSON_OBJECT(json_extract,'$.json_extract.robotData.request.Question.questionId'),'') != '' then nvl(GET_JSON_OBJECT(json_extract,'$.json_extract.robotData.request.Question.questionId'),'')
             else nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.rootGuid')),'')
        end as question_id,
        nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.robotId')),'') as robot_id,
        nvl(GET_JSON_OBJECT(json_extract,'$.json_extract.robotData.request.Question.audio'),'') as rcu_audio,
        nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.userId')),'') as robot_account_id,
        nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.serviceCode')),'') as service_code,
        nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.tenantId')),'') as tenant_id,
        nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.robotType')),'') as robot_type,
        nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.rodType')),'') as rod_type,
        nvl(GET_JSON_OBJECT(json_extract,'$.json_extract.robotData.request.Question.qaFlag'),'') as qa_flag,
        regexp_replace(nvl(GET_JSON_OBJECT(json_extract,'$.json_extract.robotData.request.Question.text'),''), '\r|\n|\t', '') as question_text,
        case when nvl(GET_JSON_OBJECT(GET_JSON_OBJECT(json_extract,'$.json_extract.robotData.request.Question.lang'),'$.lang'),'') != '' then nvl(GET_JSON_OBJECT(GET_JSON_OBJECT(json_extract,'$.json_extract.robotData.request.Question.lang'),'$.lang'),'')
             when nvl(regexp_extract(GET_JSON_OBJECT(json_extract,'$.json_extract.robotData.request.Question.lang'),'"lang":"(.+?)"}'),'') != '' then regexp_extract(GET_JSON_OBJECT(json_extract,'$.json_extract.robotData.request.Question.lang'),'"lang":"(.+?)"}')
             else nvl(GET_JSON_OBJECT(json_extract,'$.json_extract.robotData.request.Question.lang'),'')
        end as question_language,
        regexp_replace(nvl(tts.tts_text,''), '\r|\n|\t', '') as answer_text,
        case when nvl(GET_JSON_OBJECT(tts.tts_language,'$.lang'),'') != '' then nvl(GET_JSON_OBJECT(tts.tts_language,'$.lang'),'')
             when nvl(regexp_extract(tts.tts_language,'"lang":(.+?),'),'') != '' then regexp_extract(tts.tts_language,'"lang":(.+?),')
             else nvl(tts.tts_language,'')
        end as answer_language,
        nvl(tts.tts_step,'') as tts_step,
        case when GET_JSON_OBJECT(tts.tts_action,'$.param.intent.Kind.StringValue') is not null then nvl(GET_JSON_OBJECT(tts.tts_action,'$.param.intent.Kind.StringValue'),'')
             else nvl(GET_JSON_OBJECT(tts.tts_action,'$.param.intent'),'')
        end as intent_name,
        nvl(tts.tts_emoji,'') as tts_emoji,
        case when nvl(tts.tts_payload,'') = '' or nvl(tts.tts_payload,'') = '{}' then ''
             else nvl(tts.tts_payload,'')
        end as tts_payload,
        case when GET_JSON_OBJECT(tts.tts_action,'$.param.duration.Kind.NumberValue') is not null then cast(nvl(GET_JSON_OBJECT(tts.tts_action,'$.param.duration.Kind.NumberValue'),0.0) as double)
             else cast(nvl(GET_JSON_OBJECT(tts.tts_action,'$.param.duration'),0.0) as double)
        end as action_duration,
        case when GET_JSON_OBJECT(tts.tts_action,'$.param.frame_no.Kind.NumberValue') is not null then cast(nvl(GET_JSON_OBJECT(tts.tts_action,'$.param.frame_no.Kind.NumberValue'),0.0) as double)
             else cast(nvl(GET_JSON_OBJECT(tts.tts_action,'$.param.frame_no'),0.0) as double)
        end as action_frame_no,
        case when GET_JSON_OBJECT(tts.tts_action,'$.param.play_type.Kind.StringValue') is not null then nvl(GET_JSON_OBJECT(tts.tts_action,'$.param.play_type.Kind.StringValue'),'')
             else nvl(GET_JSON_OBJECT(tts_action,'$.param.play_type'),'')
        end as action_play_type,
        case when GET_JSON_OBJECT(tts.tts_action,'$.param.video_url.Kind.StringValue') is not null then nvl(GET_JSON_OBJECT(tts.tts_action,'$.param.video_url.Kind.StringValue'),'')
             else nvl(GET_JSON_OBJECT(tts_action,'$.param.video_url'),'')
        end as action_video_url,
        case when GET_JSON_OBJECT(tts.tts_action,'$.param.guide_tip.Kind.StringValue') is not null then nvl(GET_JSON_OBJECT(tts.tts_action,'$.param.guide_tip.Kind.StringValue'),'')
             else nvl(GET_JSON_OBJECT(tts_action,'$.param.guide_tip'),'')
        end as action_guide_tip,
        case when GET_JSON_OBJECT(tts.tts_action,'$.param.pic_url.Kind.StringValue') is not null then nvl(GET_JSON_OBJECT(tts.tts_action,'$.param.pic_url.Kind.StringValue'),'')
             else nvl(GET_JSON_OBJECT(tts_action,'$.param.pic_url'),'')
        end as action_pic_url,
        case when GET_JSON_OBJECT(tts.tts_action,'$.param.intent.Kind.StringValue') is not null then nvl(GET_JSON_OBJECT(tts.tts_action,'$.param.intent.Kind.StringValue'),'')
             else nvl(GET_JSON_OBJECT(tts_action,'$.param.intent'),'')
        end as action_intent,
        case when GET_JSON_OBJECT(tts.tts_action,'$.param.url.Kind.StringValue') is not null then nvl(GET_JSON_OBJECT(tts.tts_action,'$.param.url.Kind.StringValue'),'')
             else nvl(GET_JSON_OBJECT(tts_action,'$.param.url'),'')
        end as action_url,
        case when GET_JSON_OBJECT(tts.tts_action,'$.display.Kind.StringValue') is not null then nvl(GET_JSON_OBJECT(tts.tts_action,'$.display.Kind.StringValue'),'')
             else nvl(GET_JSON_OBJECT(tts_action,'$.display'),'')
        end as action_display,
        case when GET_JSON_OBJECT(tts.tts_action,'$.name.Kind.StringValue') is not null then nvl(GET_JSON_OBJECT(tts.tts_action,'$.name.Kind.StringValue'),'')
             else nvl(GET_JSON_OBJECT(tts.tts_action,'$.name'),'')
        end as action_name,
        nvl(tts.tts_audio,'') as tts_audio,
        nvl(tts.tts_type,'') as tts_type,
        nvl(k8s_svc_name,'') as k8s_svc_name,
        nvl(k8s_env_name,'') as k8s_env_name,
        event_time,
        json_extract as ext,
        case when length(GET_JSON_OBJECT(json_extract,'$.json_extract.timestamp')) = 13 then from_unixtime( cast(GET_JSON_OBJECT(json_extract,'$.json_extract.timestamp')/1000 as int),'yyyy-MM-dd')
             when length(GET_JSON_OBJECT(json_extract,'$.json_extract.timestamp')) = 10 then from_unixtime( cast(GET_JSON_OBJECT(json_extract,'$.json_extract.timestamp') as int),'yyyy-MM-dd')
             else SUBSTRING(GET_JSON_OBJECT(json_extract,'$.json_extract.timestamp'),0,10)
        end as dt
    from reception_send_tmp x1 lateral view cdmudf.tts(GET_JSON_OBJECT(json_extract,'$.json_extract.robotData.request.Answer[0].tts')) tts as tts_emoji,tts_payload,tts_action,tts_text,tts_audio,tts_language,tts_type,tts_step
),
-- 与send_qa相比，部分数据存在缺失为action_id 不为空的情况，但是action_id 不为空的总计有1300多条记录
-- 极少数据是reception_qa存在 action_qa不存在，与send_qa相比数据量少了1000多条
-- 864972049990562_1628410024385_1422a7c1  NULL
-- NULL    CC1BE0B02BC9_1628386734648_27e6cdd2
-- 存在重复数据，需要去重
reception_send_remove_duplicate as (
    select
        question_id,
        robot_id,
        rcu_audio,
        robot_account_id,
        service_code,
        tenant_id,
        robot_type,
        rod_type,
        qa_flag,
        question_text,
        question_language,
        answer_text,
        answer_language,
        tts_step,
        intent_name,
        tts_emoji,
        tts_payload,
        action_duration,
        action_frame_no,
        action_play_type,
        action_video_url,
        action_guide_tip,
        action_pic_url,
        action_intent,
        action_url,
        action_display,
        action_name,
        tts_audio,
        tts_type,
        k8s_svc_name,
        k8s_env_name,
        event_time,
        ext,
        dt,
        row_number() over(partition by question_id,question_text,answer_text order by (case when rcu_audio != '' then 0 else 1 end),event_time asc) as rnk
    from reception_send
),
-- 合并tts_step 每条question_id 一条记录
reception_send_merge as (
    select
        question_id,
        max(robot_id) as robot_id,
        max(rcu_audio) as rcu_audio,
        max(robot_account_id) as robot_account_id,
        max(service_code) as service_code,
        max(tenant_id) as tenant_id,
        max(robot_type) as robot_type,
        max(rod_type) as rod_type,
        max(qa_flag) as qa_flag,
        max(trim(question_text)) as question_text,
        max(trim(question_language)) as question_language,
        case when max(trim(answer_text)) != '' then concat_ws('@@@',collect_set(concat(case when tts_step != '' then tts_step else '0' end,'###',answer_text)))
             else ''
        end as answer_text,
        case when max(trim(answer_language)) != '' then concat_ws('@@@',collect_set(concat(case when tts_step != '' then tts_step else '0' end,'###',answer_language)))
             else ''
        end as answer_language,
        case when max(tts_step) != '' then concat_ws('@@@',collect_set(tts_step))
             else '0'
        end as tts_step,
        case when max(tts_step) != '' then concat_ws('@@@',collect_set(concat(case when tts_step != '' then tts_step else '0' end,'###',intent_name)))
             else ''
        end as intent_name,
        case when max(tts_step) != '' then concat_ws('@@@',collect_set(concat(case when tts_step != '' then tts_step else '0' end,'###',tts_emoji)))
             else ''
        end as tts_emoji,
        case when max(tts_step) != '' then concat_ws('@@@',collect_set(concat(case when tts_step != '' then tts_step else '0' end,'###',tts_payload)))
             else ''
        end as tts_payload,
        case when max(tts_step) != '' then concat_ws('@@@',collect_set(concat(case when tts_step != '' then tts_step else '0' end,'###',cast(action_duration as string))))
             else ''
        end as action_duration,
        case when max(tts_step) != '' then concat_ws('@@@',collect_set(concat(case when tts_step != '' then tts_step else '0' end,'###',cast(action_frame_no as string))))
             else ''
        end as action_frame_no,
        case when max(tts_step) != '' then concat_ws('@@@',collect_set(concat(case when tts_step != '' then tts_step else '0' end,'###',action_play_type)))
             else ''
        end as action_play_type,
        case when max(tts_step) != '' then concat_ws('@@@',collect_set(concat(case when tts_step != '' then tts_step else '0' end,'###',action_video_url)))
             else ''
        end as action_video_url,
        case when max(tts_step) != '' then concat_ws('@@@',collect_set(concat(case when tts_step != '' then tts_step else '0' end,'###',action_guide_tip)))
             else ''
        end as action_guide_tip,
        case when max(tts_step) != '' then concat_ws('@@@',collect_set(concat(case when tts_step != '' then tts_step else '0' end,'###',action_pic_url)))
             else ''
        end as action_pic_url,
        case when max(tts_step) != '' then concat_ws('@@@',collect_set(concat(case when tts_step != '' then tts_step else '0' end,'###',action_intent)))
             else ''
        end as action_intent,
        case when max(tts_step) != '' then concat_ws('@@@',collect_set(concat(case when tts_step != '' then tts_step else '0' end,'###',action_url)))
             else ''
        end as action_url,
        case when max(tts_step) != '' then concat_ws('@@@',collect_set(concat(case when tts_step != '' then tts_step else '0' end,'###',action_display)))
             else ''
        end as action_display,
        case when max(tts_step) != '' then concat_ws('@@@',collect_set(concat(case when tts_step != '' then tts_step else '0' end,'###',action_name)))
             else ''
        end as action_name,
        case when max(tts_step) != '' then concat_ws('@@@',collect_set(concat(case when tts_step != '' then tts_step else '0' end,'###',tts_audio)))
             else ''
        end as tts_audio,
        case when max(tts_step) != '' then concat_ws('@@@',collect_set(concat(case when tts_step != '' then tts_step else '0' end,'###',tts_type)))
             else ''
        end as tts_type,
        max(k8s_svc_name) as k8s_svc_name,
        max(k8s_env_name) as k8s_env_name,
        max(event_time) as event_time,
        max(ext) as ext,
        max(dt) as dt
    from reception_send_remove_duplicate
    where rnk = 1
    group by question_id
)

insert overwrite table cdmdwd.dwd_harix_cmd_reception_send_qa_i_d partition(dt)
    select
        question_id,
        robot_id,
        rcu_audio,
        robot_account_id,
        service_code,
        tenant_id,
        robot_type,
        rod_type,
        qa_flag,
        question_text,
        question_language,
        answer_text,
        answer_language,
        tts_step,
        intent_name,
        tts_emoji,
        tts_payload,
        action_duration,
        action_frame_no,
        action_play_type,
        action_video_url,
        action_guide_tip,
        action_pic_url,
        action_intent,
        action_url,
        action_display,
        action_name,
        tts_audio,
        tts_type,
        k8s_svc_name,
        k8s_env_name,
        event_time,
        ext,
        dt
    from reception_send_merge;