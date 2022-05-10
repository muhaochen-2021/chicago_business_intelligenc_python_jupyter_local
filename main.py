######################## try test
# import get_data
# import report_airport
# import report_alert
# import report_ccvi
# import report_construction
# import report_infra_investment
# import report_loan
# import report_streetcaping
# # get data
# wait_hour = 24
# get_data.get_data_star(wait_hour)
# # generate report
# rp_airport_rt = report_airport.rp_airport()
# rp_alert_rt = report_alert.rp_alert()
# rp_ccvi_rt = report_ccvi.rp_ccvi()
# rp_construction_rt = report_construction.rp_construction()
# rp_infra_rt = report_infra_investment.rp_infra()
# rp_loan_rt = report_loan.rp_loan()
# rp_street_rt = report_streetcaping.rp_street()

# print(rp_airport_rt,rp_alert_rt,rp_ccvi_rt,rp_construction_rt,rp_infra_rt,rp_loan_rt,rp_street_rt)


# main.py
import get_data
import report_airport
import report_alert
import report_ccvi
import report_construction
import report_infra_investment
import report_loan
import report_streetcaping
from nameko.rpc import rpc

class get_data_service:
    name = "get_data_service"

    @rpc
    def get__data(self):
        wait_hour = 24
        return get_data.get_data_star(wait_hour)

class generate_report_airport:
    name = "generate_report_airport"

    @rpc
    def generate_report_airport_s(self):
        return report_airport.rp_airport()

class generate_report_alert:
    name = "generate_report_alert"

    @rpc
    def generate_report_alert_s(self):
        return report_alert.rp_alert()

class generate_report_ccvi:
    name = "generate_report_ccvi"

    @rpc
    def generate_report_ccvi_s(self):
        return report_ccvi.rp_ccvi()

class generate_report_construction:
    name = "generate_report_construction"

    @rpc
    def generate_report_construction_s(self):
        return report_construction.rp_construction()

class generate_report_infra_investment:
    name = "generate_report_infra_investment"

    @rpc
    def generate_report_infra_investment_s(self):
        return report_infra_investment.rp_infra()

class generate_report_loan:
    name = "generate_report_loan"

    @rpc
    def generate_report_loan_s(self):
        return report_loan.rp_loan()

class generate_report_streetcaping:
    name = "generate_report_streetcaping"

    @rpc
    def generate_report_streetcaping_s(self):
        return report_streetcaping.rp_street()