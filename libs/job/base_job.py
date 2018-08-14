BASE_JOB = {
    'meta': {
        # 'account': account_id,
        # 'vendor': vendor_id,
        # 'audience': 'ht' | 'rmkt',
        # 'os': 'ios' | 'android',
    },
    'data': {
        'use_meta': True, # use meta info for filters
        'pos_proportion': 1,
        'neg_proportion': 2,
        'filters': []
    },
    'model': {
        'name': 'lr',
        'batch_size': 256,
        'learning_rate': 0.001,
        'epoch': 2,
        'l2': 0.001
     }
}
