# 2024-alaska-elections
Generates summaries for all state candidates in the November 2024 elections in Alaska. 

# APOC
Data from the [Alaska Public Offices Commission](https://aws.state.ak.us/ApocReports/Campaign/).
This organizes and analyzes all of the APOC campaign finance information for
Alaska House and Alaska Senate candidates, taking a csv file from the APOC campaign
finance database, cleaning and standardizing it, and creating a plain text
file with summaries of each candidate's cash contributions, in-kind donations,
and expenditures. It also creates two csv files that only contain the contributions
that totaled at least $500 in the reporting period, and the expenses that
totaled at least $1,000 in the reporting period.
