from compute.models import Stat
from rest_framework import serializers
import compute.hive_qurey as hive
import compute.insert_result_mysql as mysql

class StatSerializer(serializers.ModelSerializer):

    return_msg = serializers.SerializerMethodField()

    class Meta:
        model = Stat
        fields = ('dataSetUID', 'DB', 'TABLE', 'return_msg')

    def get_return_msg(self, obj):

        if obj.DB == "nhic": #건보
            command = "SELECT year, count(distinct key_seq) as statements, count(distinct person_id)  as patients, sum(edec_tramt) as total_expense FROM "+obj.DB+"."+obj.TABLE+" GROUP BY year"
            df = hive.hive_query(command)
            mysql.result_insert(obj.dataSetUID, df, "statistic_nhic")

        elif obj.DB == "nps": #심평원
            command = "SELECT year, count(distinct key_seq) as statements, count(distinct person_id)  as patients, sum(rvd_rpe_tamt) as total_expense FROM "+obj.DB+"."+ obj.TABLE +" GROUP BY year"
            df = hive.hive_query(command)
            mysql.result_insert(obj.dataSetUID, df, "statistic_hira")

        elif obj.DB == "koges" :
            command1 = "select count(*) as total, sum(if(a00_sex=1,1,0)) as male, sum(if(a00_sex=2,1,0)) as female from "+obj.DB+"."+obj.TABLE
            epi_df = hive.hive_query_koges_epi(command1)
            mysql.result_insert_koges(obj.dataSetUID, epi_df, "statistic_koges","NULL","1")

            command2 = "desc "+obj.DB+"."+obj.TABLE
            snp = hive.koges_snp_name(command2)
            df = hive.koges_snp_result(snp, obj.DB, obj.TABLE)
            mysql.result_insert_koges(obj.dataSetUID, df, "statistic_koges", snp, "2")

        else :
            raise NotImplementedError

        result = "complete"

        return (result)