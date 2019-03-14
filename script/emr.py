#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__ = 'mark'
__email__ = 'mark@zamplus.com'

import json
import boto3
from datetime import datetime
class EMR:
    def __init__(self):
        self._emr = boto3.client('emr')


    def active_cluster_id(self, created_after=datetime.strptime("1980-01-01","%Y-%m-%d"), cluster_name='zampda_model_release',states=['RUNNING', 'WAITING']):
        cluster_id = None
        ret = self._emr.list_clusters(CreatedAfter=created_after, ClusterStates=states)
        if len(ret['Clusters']) >0:
            for cluster in ret['Clusters']:
                if cluster_name is not None:
                    if cluster['Name'] == cluster_name:
                        cluster_id = cluster['Id']
                        return cluster_id
                else:
                    cluster_id = cluster['Id']
                    return cluster_id
        else:
            return None

    def master_public_ip(self, cluster_id):
        return self._emr.describe_cluster(ClusterId=cluster_id)['Cluster']['MasterPublicDnsName']

    @staticmethod
    def create_cluster():
        # create emr cluster
        ec2_attributes = {
            "KeyName": "model-emr",
            "InstanceProfile": "EMR_EC2_DefaultRole",
            "SubnetId": "subnet-fff32c96",
            "EmrManagedSlaveSecurityGroup": "sg-5b087732",
            "EmrManagedMasterSecurityGroup": "sg-3c077855"
        }

        instance_group = [
            {
                "InstanceCount": 1,
                "BidPrice": "0.16",
                "EbsConfiguration": {
                    "EbsBlockDeviceConfigs": [
                        {
                            "VolumeSpecification": {
                                "SizeInGB": 32,
                                "VolumeType": "gp2"
                            },
                            "VolumesPerInstance": 1
                        }
                    ]
                },
                "InstanceGroupType": "MASTER",
                "InstanceType": "c4.large",
                "Name": "master-1"
            },
            {
                "InstanceCount": 3,
                "BidPrice": "0.16",
                "EbsConfiguration": {
                    "EbsBlockDeviceConfigs": [
                        {
                            "VolumeSpecification": {
                                "SizeInGB": 32,
                                "VolumeType": "gp2"
                            },
                            "VolumesPerInstance": 1
                        }
                    ]
                },
                "InstanceGroupType": "CORE",
                "InstanceType": "c4.large",
                "Name": "core-1"
            }
        ]

        cmd = [
            'aws emr create-cluster',
            '--applications Name=Hadoop Name=Spark',
            '--ebs-root-volume-size 10',
            f'--ec2-attributes \'{json.dumps(ec2_attributes)}\'',
            '--service-role EMR_DefaultRole',
            '--enable-debugging',
            '--release-label emr-5.10.0',
            '--log-uri \'s3n://pctr/log\'',
            '--name \'pctr-trainning\'',
            f'--instance-groups \'{json.dumps(instance_group)}\'',
            '--scale-down-behavior TERMINATE_AT_TASK_COMPLETION',
            '--region cn-northwest-1'
        ]

        cmd = ' '.join(cmd)
        print(cmd)
