from libs.task.task import Task



g_task_dict ={
    'name':'test_job',
    'task_args':{ 'interval':30,'account': 20, 'vendor': 24},
    'data_split':
        {
            'mode': 'date',
            'args':{
            'train_start_date':'2019-03-23',
            'train_end_date':'2019-03-26',
            'test_start_date': '2019-03-27',
            'test_end_date': '2019-03-28'}
        },
    'features_base':
        {
            'type': 'RTBModelBaseDataSource',
            'name': "rtb",
            'global_filter' :['Win_Price > 0', "Device_Os='android'", 'has(Segment.Id,100020)=1 '],
            'overwrite':False,
            'train_args':
                {
                },
            'test_args':
                {
                }
        },
    'features_extend':[
        { "features_name":"av_ctr_day_interval{interval}",'args':{'interval':30},
          'keys':["Id_Zid","Media_VendorId","Bid_CompanyId","EventDate"],'overwrite':False,
          'imp':[
              {'imp': 'onehot', 'col_name': 'a{account}_v{vendor}_last{interval}_imp'},
              {'imp': 'onehot', 'col_name': 'a{account}_v{vendor}_last{interval}_clk'},
              {'imp': 'onehot', 'col_name': 'a{account}_v{vendor}_last{interval}_ctr'},
            ]},
        # {"features_name": "AdImage",
        #  "features_class": "AdImage",
        #  'keys': ["Bid_AdId"], 'overwrite': False,
        #  'imp': [
        #      {'imp': 'vector', 'col_name': 'adimage'},
        #  ]},
        {"features_name": "qu_gid_seq_vec16",
         "features_class": "AdidVecDataSource",
         'keys': ["Id_Zid"], 'overwrite': True,
         'imp': [
            # {'imp': 'vector', 'col_name': 'adid_clk_vec_avg'},
            {'imp': 'vector', 'col_name': 'adid_vec_avg'},
         ]},

    ],
    # 'features_transform':[
    #     {"features_name": "yt_adid_tran_vec8",
    #      "features_class": "AdidVecTranform",
    #      'keys': ["Id_Zid"], 'overwrite': True,
    #      'imp': [
    #         {'imp': 'vector', 'col_name': 'Bid_AdId_emb'},
    #      ]}
    # ],

    'features_processing':{
        'label':'is_clk',
        'cols':
        [
            # {'imp': 'int', 'col_name': 'Time_Hour'},
            # {'imp': 'onehot', 'col_name': 'Time_Hour'},
            # {'imp': 'onehot', 'col_name': 'Age'},
        ]
    },
    'model':[
        {'trainer':"tflr",'trainer_params':{},
         }
    ]

}


model_task = Task(g_task_dict)
model_task.run()