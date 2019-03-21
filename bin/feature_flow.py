from libs.task.task import Task



g_task_dict ={
    'name':'test_job',
    'task_args':{ 'interval':10,'account': 20, 'vendor': 24},
    'data_split':
        {
            'mode': 'date',
            'args':{
            'train_start_date':'2019-03-11',
            'train_end_date':'2019-03-14',
            'test_start_date': '2019-03-15',
            'test_end_date': '2019-03-15'}
        },
    'features_base':
        {
            'type': 'RTBModelBaseDataSource',
            'name': "rtb",
            'global_filter' :['Win_Price > 0', "Device_Os='android'", 'has(Segment.Id, 100012)=0 '],
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
          'processing':[
              {'processing': 'onehot', 'col_name': 'a{account}_v{vendor}_last{interval}_imp'},
              {'processing': 'onehot', 'col_name': 'a{account}_v{vendor}_last{interval}_clk'},
              {'processing': 'onehot', 'col_name': 'a{account}_v{vendor}_last{interval}_ctr'},
            ]},
        # {"features_name": "AdImage",
        #  'keys': ["Bid_AdId"], 'overwrite': False,
        #  'processing': [
        #      {'processing': 'vector', 'col_name': 'adimage'},
        #  ]},
        {"features_name": "AdidVecDataSource",
         'keys': ["Id_Zid"], 'overwrite': False,
         'processing': [
             {'processing': 'vector', 'col_name': 'adid_vec_avg'},
         ]},
    ],

    'features_processing':{
        'label':'is_clk',
        'cols':
        [
            # {'processing': 'int', 'col_name': 'Time_Hour'},
            # {'processing': 'onehot', 'col_name': 'Time_Hour'},
            # {'processing': 'onehot', 'col_name': 'Age'},
        ]
    },
    'model':[
        {'trainer':"tflr",'trainer_params':{},
         }
    ]

}


model_task = Task(g_task_dict)
model_task.run()