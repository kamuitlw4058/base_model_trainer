from script.emr import EMR
from datetime import datetime,timedelta


emr = EMR()
cluster_id = emr.active_cluster_id(datetime.today() - timedelta(days=2))
print(cluster_id)