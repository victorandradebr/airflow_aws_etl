EMR_CONFIG = {
    'Name': 'ETL-VICTOR-AWS',
    "ReleaseLabel": "emr-6.5.0",
    "Applications": [{"Name": "Hadoop"}, {"Name": "Spark"}], 
    'Instances': {
        'InstanceGroups': [
            {
                'Name': 'MASTER_NODES',
                'Market': 'ON_DEMAND',
                'InstanceRole': 'MASTER',
                'InstanceType': 'm5.xlarge',
                'InstanceCount': 1,
            },
            {
                "Name": "CORE_NODES",
                "Market": "ON_DEMAND",
                "InstanceRole": "CORE",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "TASK_NODES",
                "Market": "SPOT",
                "BidPrice": "0.088",
                "InstanceRole": "TASK",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
                "AutoScalingPolicy":
                    {
                        "Constraints":
                    {
                        "MinCapacity": 1,
                        "MaxCapacity": 2
                    },
                    "Rules":
                        [
                    {
                    "Name": "Scale Up",
                    "Action":{
                        "SimpleScalingPolicyConfiguration":{
                        "AdjustmentType": "CHANGE_IN_CAPACITY",
                        "ScalingAdjustment": 1,
                        "CoolDown": 120
                        }
                    },
                    "Trigger":{
                        "CloudWatchAlarmDefinition":{
                        "ComparisonOperator": "GREATER_THAN_OR_EQUAL",
                        "EvaluationPeriods": 1,
                        "MetricName": "Scale Up",
                        "Period": 60,
                        "Threshold": 15,
                        "Statistic": "AVERAGE",
                        "Threshold": 75
                        }
                    }
                    }
                    ]
                }
            }
        ],
        'KeepJobFlowAliveWhenNoSteps': True,
        'TerminationProtected': False,
        'Ec2SubnetId': 'subnet-0c6f2b6940aabf014'  # SUBNET PUBLICA
    },
    'VisibleToAllUsers': True,
    'JobFlowRole': 'EMR_EC2_DefaultRole',
    'ServiceRole': 'EMR_DefaultRole',
    'AutoScalingRole': 'EMR_AutoScaling_DefaultRole',
    'StepConcurrencyLevel': 1
}