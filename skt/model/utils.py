from collections import Counter
from skt.ye import hive_to_pandas, slack_send


# post_filter 이후, context_mapping 을 마친 post_filter_with_context_id Table 생성
def context_mapping(
    meta_table=None,
    comm_db=None,
    reco_type=None,
    model_name=None,
    dt=None,
    feature_ym=None
):

    # 1. item_reco_predict_post_filter with impression 'Y' table LOAD
    query = f"""
    select *
    from {comm_db}.item_reco_predict_post_filter
    where reco_type = '{reco_type}'
    and model = '{model_name}'
    and dt = '{dt}'
    and impression_yn = 'Y'
    """
    post_filter_with_y = hive_to_pandas(query)
    print('------------------' + 'predict_post_filter' + ' WITH "Y"' + ' loaded')

    # 2. context_meta Table LOAD
    query = f"""
    select * from {meta_table}
    where item_id = '{post_filter_with_y['prod_id'][0]}'
    """
    meta = hive_to_pandas(query)
    print('------------------' + 'meta_table' + ' WITH "ITEM_ID (PROD_ID)"' + ' loaded')

    CONTEXT_NUM = meta.shape[0]
    CONTEXT_ID_DEFAULT = meta[meta['context_priority'] == 1]['context_id'].values[0]
    CONTEXT_ID_LIST = tuple(meta['context_id'].values)

    # 3. default_setting
    post_filter_with_y['context_id'] = CONTEXT_ID_DEFAULT
    print('------------------' + 'CONTEXT_NUM : 1 (DEFAULT)')

    if CONTEXT_NUM == 1:
        context_count_list = list(Counter(list(post_filter_with_y['context_id'].values)).items())
        msg = ('[CONTEXT-MAPPING-SUMMARY]' + '\n' + str(context_count_list[0][0]) + ' : ' + str(context_count_list[0][1]))

    # 4. context_num >= 2 : context mapping (with context priority)
    else:
        query = f"""
        select svc_mgmt_num, dimension
        from    comm.user_profile_derivative_monthly
        where   ym = '{feature_ym}'
        and     value = 'Y'
        and     dimension in {CONTEXT_ID_LIST}
        """
        df_context = hive_to_pandas(query)

        # 4+a. for loop - overwrite mapping
        for i in range(CONTEXT_NUM - 1):
            priority_num = i + 2
            print('------------------' + 'CONTEXT_NUM : ' + str(priority_num))
            PRIORITY_CONTEXT_ID = meta[meta['context_priority'] == priority_num]['context_id'].values[0]
            post_filter_with_y.loc[post_filter_with_y['svc_mgmt_num'].isin(list(df_context[df_context['dimension'] == PRIORITY_CONTEXT_ID]['svc_mgmt_num'])), "context_id"] = PRIORITY_CONTEXT_ID

        # 4+b. counter - context mapping statistics
        context_count_list = list(Counter(list(post_filter_with_y['context_id'].values)).items())
        msg = '[CONTEXT-MAPPING-SUMMARY]'
        for j in range(CONTEXT_NUM):
            msg_ = ('\n' + str(context_count_list[j][0]) + ' : ' + str(context_count_list[j][1]))
            msg += msg_

    # 5. Slack Report
    slack_send(text=msg, channel="#rec_modeling_alert", icon_emoji=':mag:')

    # 6. return post_filter_with_y_with_context_id Table
    return post_filter_with_y
