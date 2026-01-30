import pandas as pd
import time
import string
from thefuzz import fuzz

# Shows all columns with line breaks when printing a dataframe.
pd.set_option("display.max_columns",None)



# Sets the report and election to summarize!

# writing_report options are "Thirty Day", "Seven Day" and "Year Start"
writing_report = "Seven Day"

# writing_election options are "State General" and "State Primary"
writing_election = "State General"



def read_function(
        file_path: str
        ):
    """Reads the given csv file into a dataframe, and returns that dataframe.
    
    Parameters
    ----------
    file_path : 
        The file path of the .csv file to read.
    """

    print("Attempting to create data frame...")

    # Grabs system time before reading, to calculate how long the read took
    import_start = time.time()

    # Creates and fills data frame with the filing information
    df = pd.read_csv(file_path,
                     dtype={
                         "Result": int,
                         "Date": str,
                         "Transaction Type": str,
                         "Payment Type": str,
                         "Payment Detail": str,
                         "Amount": str,
                         "Last/Business Name": str,
                         "First Name": str,
                         "Address": str,
                         "City": str,
                         "State": str,
                         "Zip": str,
                         "Country": str,
                         "Occupation": str,
                         "Employer": str,
                         "Purpose of Expenditure": str,
                         "--------": str,
                         "Report Type": str,
                         "Election Name": str,
                         "Election Type": str,
                         "Municipality": str,
                         "Office": str,
                         "Filer Type": str,
                         "Name": str,
                         "Report Year": int,
                         "Submitted": str
                     })

    # Grabs system time after reading, to calculate how long the read took
    import_end = time.time()

    print("Successfully created data frame!")

    print(f"Import took {round(import_end - import_start, 5)} seconds.")
    print("")

    return df
big_df = read_function("input_csvs/CD_Transactions_10-30-2024.csv")


def cleaner(
        df: pd.DataFrame
        ):
    """Return a modified df with standardized column names, transaction amounts
    stripped of extraneous characters, amounts converted to numerics and 
    expenditures made negative, to keep with the standard of positive revenue,
    negative expenditure. It also drops the column "--------" from the original
    modified df because that column entirely consists of null values, and 
    converts the entries in the "date" column into datetime data types. Creates
    a "donor_full_name" column that concatenates the "first_name" and 
    "last/business_name" columns for legibility, and creates unique donor ids
    for each donor/recipient, then uses fuzzy string matching to  
    uses fuzzy string matching to check whether the  
    

    Parameters
    ----------
    df : 
        The dataframe to clean.
    """

    # Makes column names lowercase and replaces their spaces with underscores
    df.columns = [c.lower().replace(" ", "_") for c in df.columns]

    # Renames the "name" column to "candidate_name", for clarity
    df = df.rename(columns={"name": "candidate_name"})
    
    df.amount = df.amount.replace("[,$]", "", regex=True)
    df.amount = pd.to_numeric(df.amount,errors='coerce')

    df.loc[df.transaction_type == "Expenditure", "amount"] *= -1

    df = df.drop(['--------'], axis=1)

    # Converts the "date" data from a string into a datetime datatype
    df.date = pd.to_datetime(df.date, format="%m/%d/%Y")

    # Converts the "submitted" data from a string into a datetime datatype
    df.submitted = pd.to_datetime(df.submitted, format="%m/%d/%Y")
    
    # Combines first name and last/business name, replacing na with ""
    df["donor_full_name"] = df.first_name.fillna("") + " " \
                            + df["last/business_name"].fillna("")

    # Creates a new donor_id column
    df["donor_id"] = df.fillna("").groupby(["donor_full_name"]).ngroup()

    fuzz_time_start = time.time()

    df["donor_score"] = df.apply(lambda row: fuzz.token_set_ratio(\
        row["candidate_name"], row["donor_full_name"]\
        ), axis=1)

    fuzz_time_end = time.time()
    
    print(f"Fuzzy matching took {\
        round(fuzz_time_end - fuzz_time_start, 5)} seconds.")

    df["is_self"] = df.apply(\
        lambda row: True if row["donor_score"] >= 79 else False, axis=1)

    # Reorders the columns in big_df to be closer to the order for writing
    df = df[["result", "candidate_name", "amount", "date", "transaction_type",
             "payment_type", "payment_detail", "purpose_of_expenditure", 
             "donor_full_name", "donor_id", "address", "city", "state", "zip",
             "country", "employer", "occupation", "donor_score", "is_self", 
             "report_type", "election_name", "election_type","municipality", 
             "office", "filer_type", "report_year", "submitted", "first_name", 
             "last/business_name"]]

    return df
big_df = cleaner(big_df)


def summary_dialog(
        df: pd.DataFrame
        ):
    """Print a summary of the dataframe.
    
    Parameters
    ----------
    df : 
        The dataframe to summarize."""
    print(f"Post-cleaning summary:\n")
    print(df.info())
    print("")
summary_dialog(big_df)


def office_filler(): 
    """Fills the "office" column for all candidates with no reported income
    during the analysis of the 30-day general election results. """
    big_df.loc[big_df["candidate_name"] == "Calvin Schrage", "office"] = "House"
    big_df.loc[big_df["candidate_name"] == "Denny Wells", "office"] = "House"
    big_df.loc[big_df["candidate_name"] == "Dawson R Slaughter", "office"] = "House"
    big_df.loc[big_df["candidate_name"] == "Ronald D Gillham", "office"] = "House"
    big_df.loc[big_df["candidate_name"] == "Calvin Schrage", "office"] = "House"
    big_df.loc[big_df["candidate_name"] == "Craig Johnson", "office"] = "House"
    big_df.loc[big_df["candidate_name"] == "Harry Winner Kamdem", "office"] = "House"
    big_df.loc[big_df["candidate_name"] == "Dustin T. Darden", "office"] = "House"
    big_df.loc[big_df["candidate_name"] == "William Z. \"Zack\" Fields", "office"] = "House"
    big_df.loc[big_df["candidate_name"] == "Kaylee M. Anderson", "office"] = "House"
    big_df.loc[big_df["candidate_name"] == "Russell O. Wyatt", "office"] = "House"
    big_df.loc[big_df["candidate_name"] == "Scott A Kohlhaas", "office"] = "House"
    big_df.loc[big_df["candidate_name"] == "Cathy L. Tilton", "office"] = "House"
    big_df.loc[big_df["candidate_name"] == "Wright, Jessica", "office"] = "House"
    big_df.loc[big_df["candidate_name"] == "Kevin J. McCabe", "office"] = "House"
    big_df.loc[big_df["candidate_name"] == "Joy Beth Cottle", "office"] = "House"
    big_df.loc[big_df["candidate_name"] == "James Fields", "office"] = "House"
    big_df.loc[big_df["candidate_name"] == "Dana Mock", "office"] = "House"
    big_df.loc[big_df["candidate_name"] == "Mike Cronk", "office"] = "House"
    big_df.loc[big_df["candidate_name"] == "Darren Morgan Deacon", "office"] = "House"
    big_df.loc[big_df["candidate_name"] == "Nellie D. Jimmie", "office"] = "House"
    big_df.loc[big_df["candidate_name"] == "Willy Keppel", "office"] = "House"
    big_df.loc[big_df["candidate_name"] == "Victoria Beatrice Sosa", "office"] = "House"


    big_df.loc[big_df["candidate_name"] == "Tina Wegener", "office"] = "Senate"
    big_df.loc[big_df["candidate_name"] == "Harold Borbridge", "office"] = "Senate"
    big_df.loc[big_df["candidate_name"] == "Janice Park", "office"] = "Senate"
    big_df.loc[big_df["candidate_name"] == "Cheronda L. Smith", "office"] = "Senate"
    big_df.loc[big_df["candidate_name"] == "Lee E Hammermeister", "office"] = "Senate"
    big_df.loc[big_df["candidate_name"] == "Click Bishop", "office"] = "Senate"
    big_df.loc[big_df["candidate_name"] == "Williams, Robert 'Bert'", "office"] = "Senate"
office_filler()

schrage_df = big_df[big_df.candidate_name == "Calvin Schrage"]
tilton_df = big_df[big_df.candidate_name == "Cathy L. Tilton"]

print("Tilton dataframe:")
print(tilton_df.info())

missing_df = big_df[big_df.office == ""]
print(missing_df.head())


nans = pd.isna(big_df.office)
print(nans)
print(big_df.office.unique())


def top_house_donors(num_to_show: int):
    top_house_donor_df = big_df[big_df.office == "House"]\
        .groupby(["donor_full_name"]).amount.sum().sort_values(ascending=False)
    print("Biggest House donors:")
    print(top_house_donor_df.head(num_to_show))
    print("")
#top_house_donors(10)

def top_donors(num_to_show: int):
    top_donor_df = big_df.groupby(["donor_full_name"])\
        .amount.sum().sort_values(ascending=False)
    print("Biggest overall donors:")
    print(top_donor_df.head(num_to_show))
    print("")
#top_donors(10)


# Creates two data frames to handle just the House and just the Senate 
# candidates, and drops the "municipality" column
house_df = big_df[big_df.office == "House"].drop("municipality", axis=1)
senate_df = big_df[big_df.office == "Senate"].drop("municipality", axis=1)


print(house_df.candidate_name.unique())
print(senate_df.candidate_name.unique())




# Nested lists of candidate names - a list of lists - on the principle that 
# the index of each nested list within the master list plus one should 
# correspond to the House district number (i+1). For example, the list at 
# index 3 (row four) corresponds to the candidates for House District 4. 
# Therefore, if we have the list of candidate names sorted into district lists,
# we can get a second list of districts for free!
nested_house_name_list = [
    ["Daniel (Dan) Ortiz", "Jeremy T. Bynum", "Agnes C. Moran",
        "Grant EchoHawk", "Robb Arnold"],
    ["Rebecca Himschoot"],
    ["Andrea \"Andi\" Story"],
    ["Sara Hannan"],
    ["Louise Stutes", "Leighton Radner"],
    ["Sarah L. Vance", "Alana L. Greear/AlanaforAlaska", "Brent Johnson", 
        "Dawson R Slaughter", "Michael Daniel"],
    ["Justin Ruffridge", "Ronald D Gillham"],
    ["Bill Elam", "John Hillyer"],
    ["Lucy Bauer", "Lee Ellis", "Ky Holland", "Brandy Pennington", 
        "David Lee Schaff"],
    ["Craig Johnson", "Charles \"Chuck\" Kopp ", "Greg Magee"],
    ["Julie Coulombe", "Walter Featherly"],
    ["Calvin Schrage", "Joseph Crisafi-Lurtsema"],
    ["Andrew Louis Josephson", "Heather Gottshall"],
    ["Alyse S. Galvin", "Harry Winner Kamdem"],
    ["Mia Costello", "Dustin T. Darden", "Denny Wells", "Thomas W McKay"],
    ["Carolyn Hall", "Nick Moe"],
    ["William Z. \"Zack\" Fields"],
    ["Cliff Groh", "David Nelson"],
    ["Genevieve Mina", "Kaylee M. Anderson", "Russell O. Wyatt"],
    ["Andrew T. Gray", "Scott A Kohlhaas"],
    ["Donna C Mears", "Aimee Sims"],
    ["Stanley Wright", "Ted J. Eischeid"],
    ["Jamie Allard", "Jim Arlington"],
    ["Dan Saddler"],
    ["DeLena M Johnson"],
    ["Cathy L. Tilton"],
    ["David Eastman", "Jubilee Underwood"],
    ["Jesse M. Sumner", "Steve Menard", "Elexie Moore", "Wright, Jessica"],
    ["George Rauscher", "Bruce Wall"],
    ["Kevin J. McCabe", "Doyle Holmes"],
    ["Maxine Dibert", "Barton S. LeBon"],
    ["Will Stapp", "Gary K. Damron"],
    ["Mike Prax", "Michael W. Welch"],
    ["Frank Tomaszewski", "Joy Beth Cottle"],
    ["Ashley Carrick", "Ruben A. McNeill Jr."],
    ["James Fields", "Pamela Goode", "Brandon P. Kowalski \"Putuuqti\"", 
        "Dana Mock", "Rebecca (Becky) Schwanke", "Cole Snodgress", 
        "Mike Cronk"],
    ["Bryce Edgmon", "Darren Morgan Deacon"],
    ["CJ McCormick", "Nellie Darlene Jimmie", "Willy Keppel", 
        "Victoria Beatrice Sosa"],
    ["Neal Winston Foster", "Tyler Ivanoff"],
    ["Thomas C Ikaaq Baker", "Robyn Niayuq Burke", "Saima Chase"]
]
nested_senate_name_list = [
    [],
    ["Jesse Kiehl"],
    [],
    ["Jesse J Bjorkman", "Ben Carpenter", "Andrew Cizek", "Tina Wegener"],
    [],
    ["Harold Borbridge", "James Kaufman", "Janice Park"],
    [],
    ["Matt Claman", "Liz Vazquez ", "Thomas W McKay"],
    [],
    ["Forrest Dunbar", "Cheronda L. Smith"],
    [],
    ["Kelly R. Merrick", "Jared David Goecker", "Lee E Hammermeister", 
        "Ken McCarty", "Sharon Denise Jackson"],
    [],
    ["David S. Wilson", "Wright, Stephen", "Robert D Yundt II"],
    [],
    ["Leslie Hajdukovich", "Scott Kawasaki"],
    [],
    ["Click Bishop", "Mike Cronk", "Savannah Fletcher", 
        "Williams, Robert 'Bert'", "James Squyres"],
    [],
    ["Donald \"Donny\" C. Olson"]
]

# Creates a string of uppercase alphabetical letters, to use for assigning 
# Senate district names.
senate_districts = string.ascii_uppercase[:20]

""" Creates a list of all the unique names of Alaska Legislative candidates 
based on the nested candidate name lists. Why do this by iterating through the 
nested name lists, instead of just grabbing the unique names from big_df for 
each chamber? I'm pretty sure it had to do with checking that the names in the
nested list matched the actual candidate names as registered with APOC. I'm 
also noticing that doing it this way, the list of unique candidates is 
guaranteed to be ordered in the same way as in the nested list, from lowest 
district to highest district.
"""
unique_house_names = []
for district_list in nested_house_name_list:
    for candidate in district_list:
        unique_house_names.append(candidate)
unique_senate_names = []
for district_list in nested_senate_name_list:
    for candidate in district_list:
        unique_senate_names.append(candidate)


# Defines a function to return a dictionary with each candidate's name as a key
# and the corresponding district as a value.
def district_dictionary_generator(
        chamber: str,
        name_list: list,
        reference_list: list
        ):
    """Return a dictionary that associates names with a district. Each 
    candidate's name is a key, and her corresponding House or Senate district
    is the associated value. Any candidate names not found in the reference
    list will have a value of 0 for House races and "Z" for Senate races.
    
    Parameters
    ----------
    chamber : 
        The chamber for which to generate a dictionary.
        Takes "house" or "senate" as input.
    name_list : 
        The list of unique candidate names for the given chamber.
        Compiled from the dataframe directly. This list will actually produce
        the keys in the output dictionary, associating each name with a 
        district by checking if the name is in the "reference" list of lists. 
    reference_list : 
        The nested list of lists of candidate names for each district, to get
        values for the keys in name_list.
        Formatted as a nested list order to easily get a district from each 
        name in name_list. 
    """
    dict = {}
    if chamber == "house":
        for name in name_list:
            for district in reference_list:
                if name in district:
                    dict[name] = reference_list.index(district) + 1
                    break
                elif district == reference_list[-1]:
                    dict[name] = 0
    elif chamber == "senate":
        for name in name_list:
            for district in reference_list:
                if name in district:
                    dict[name] = senate_districts[
                        reference_list.index(district)]
                    break
                elif district == reference_list[-1]:
                    dict[name] = "Z"
    else:
        print("Error: accepts only \"house\" or \"senate\" as entries.")
    return dict

# Saves the above dictionary to separate variables for the house and senate
# dictionaries.
house_district_dictionary = district_dictionary_generator(
    "house", unique_house_names, nested_house_name_list)
senate_district_dictionary = district_dictionary_generator(
    "senate", unique_senate_names, nested_senate_name_list)

def create_house_district_column():

    # Tries to create a "district" column in house_df
    print("Attempting to create a new \"district\" column in house_df...")

    # Creates a new "district" column in house_df with the House district of 
    # each candidate 
    house_df["district"] = house_df.candidate_name\
        .map(house_district_dictionary)

    print("New column \"district\" successfully created in house_df!")
    print("")

    return house_df
house_df = create_house_district_column()

def create_senate_district_column():
        
    # Tries to create a "district" column in senate_df
    print("Attempting to create a new \"district\" column in senate_df...")

    # Creates a new "district" column in senate_df with the Senate district of 
    # each candidate 
    senate_df["district"] = senate_df.candidate_name\
        .map(senate_district_dictionary)

    print("New column \"district\" successfully created in senate_df!")
    print("")

    return senate_df
senate_df = create_senate_district_column()

# Having added a district column, creates a master dictionary that stores the
# dataframes for all House candidates.
master_house_df_dictionary = {}
for name in house_district_dictionary:
    master_house_df_dictionary[name] = \
        house_df[house_df.candidate_name == name]

# Creates a master dictionary that stores the dataframes for all Senate 
# candidates.
master_senate_df_dictionary = {}
for name in senate_district_dictionary:
    master_senate_df_dictionary[name] = \
        senate_df[senate_df.candidate_name == name]






def pick_a_district(
        house_or_senate: str,
        district: int | str,
        report: str,
        election: str
        ):
    """For the given district, print summaries of all candidates in the 
    district.

    Parameters
    ----------
    house_or_senate : 
        Are we summarizing a House district, or a Senate district?
        House district is an integer, 1 through 40.
        Senate district is an uppercase letter string, "A" through "T".
    district : 
        asdfasd
    report : 
        asdfasdf
    election : 
        asdfasdfsadf
    """
    
    # Creates an empty dictionary for all transactions for each candidate in 
    # the district
    district_dictionary_all = {}

    # Creates an empty dictionary for only income transactions for each 
    # candidate in the district
    district_dictionary_revenue = {}

    # Creates an empty dictionary for only "Non-Monetary" contributions 
    # for each candidate in the district
    district_dictionary_in_kind = {}

    # Creates an empty dictionary for only expense transactions for each 
    # candidate in the district
    district_dictionary_expenditure = {}

    if house_or_senate == "house":
        print(f"Summary for House District {district}:")
        print("")

        # Defines a list of the candidates in the given House district, taken
        # from the master list of lists
        district_candidates = nested_house_name_list[district-1]

        # Populates the "all transactions" dictionary with dataframes for 
        # each candidate 
        for candidate_name in district_candidates:
            district_dictionary_all[candidate_name] = \
            master_house_df_dictionary[candidate_name]\
                [\
                    (master_house_df_dictionary[candidate_name]\
                     .report_type.str.contains(report))\
                     & (master_house_df_dictionary[candidate_name]\
                        .report_type.str.contains(election))\
                ]
        
        # Populates the "revenue" dictionary
        for candidate_name in nested_house_name_list[district-1]:
            district_dictionary_revenue[candidate_name] = \
                district_dictionary_all[candidate_name]\
                    [\
                    (district_dictionary_all[candidate_name]\
                    .transaction_type == "Income")\
                    & (district_dictionary_all[candidate_name]\
                    .payment_type != "Non-Monetary")
                    ]
            
        # Populates the "in_kind" dictionary
        for candidate_name in nested_house_name_list[district-1]:
            district_dictionary_in_kind[candidate_name] = \
                district_dictionary_all[candidate_name]\
                    [\
                    (district_dictionary_all[candidate_name]\
                    .transaction_type == "Income")
                    & (district_dictionary_all[candidate_name]\
                    .payment_type == "Non-Monetary")
                    ]

        # Populates the "all expenses" dictionary with dataframes 
        # for each candidate
        for candidate_name in nested_house_name_list[district-1]:
            district_dictionary_expenditure[candidate_name] = \
            district_dictionary_all[candidate_name][\
                district_dictionary_all[candidate_name].transaction_type\
                == "Expenditure"\
                ]
        
    elif house_or_senate == "senate":
        print(f"Summary for Senate District {district}:")
        print("")

        # Defines a list of the candidates in the given Senate district, 
        # taken from the master list of lists
        district_candidates = \
            nested_senate_name_list[senate_districts.index(district)]

        # Populates the "all transactions" dictionary with dataframes for 
        # each candidate 
        for candidate_name in district_candidates:
            district_dictionary_all[candidate_name] = \
                master_senate_df_dictionary[candidate_name]
        
        # Populates the "revenue" dictionary with dataframes for each candidate
        for candidate_name in nested_senate_name_list[
            senate_districts.index(district)]:
            district_dictionary_revenue[candidate_name] = \
                district_dictionary_all[candidate_name][\
                district_dictionary_all[candidate_name]\
                .transaction_type == "Income"\
                ]

        # Populates the "all expenses" dictionary with dataframes for each 
        # candidate
        for candidate_name in \
        nested_senate_name_list[senate_districts.index(district)]:
            district_dictionary_expenditure[candidate_name] = \
            district_dictionary_all[candidate_name]\
                [\
                    district_dictionary_all[candidate_name]\
                    .transaction_type == "Expenditure"\
                ]  
    
    # Prints summary statistics for each candidate in the district
    for key in district_dictionary_revenue.keys():
        # Candidate header
        print(f"{key}")
        print("=================")

        # If there are no recorded transactions for the candidate
        if district_dictionary_revenue[key].empty:
            print(f"There are no transactions recorded for {key}.")
            print("")
        else:
            # Total number of contributions
            print(f"{key}'s campaign received {\
                district_dictionary_revenue[key]\
                .amount.count()} contributions.")
            
            # Total revenue
            print(f"{key}'s campaign received ${\
                district_dictionary_revenue[key]\
                .amount.sum()} for the reporting period.")
            
            # Average donation amount
            print(f"The average contribution to {key}'s campaign was ${\
                round(district_dictionary_revenue[key].amount.mean(), 2)}.")
            
            # Median donation amount
            print(f"The median contribution to {key}'s campaign was ${\
                district_dictionary_revenue[key].amount.median()}.")
            
            # Minimum donation amount
            print(f"The minimum contribution to {key}'s campaign was ${\
                district_dictionary_revenue[key].amount.min()}.")
            
            # List minimal donations
            print("The minimum contribution came from the following:")
            print(district_dictionary_revenue[key]\
                  [\
                    (district_dictionary_revenue[key].amount \
                    == district_dictionary_revenue[key].amount.min())\
                  ]\
                .drop(columns=["last/business_name", "first_name"]))

            # Maximum donation amount
            print(f"The maximum contribution to {key}'s campaign was ${\
                district_dictionary_revenue[key]\
                .amount.max()}.")
            
            # List maximal donations
            print("The maximum contribution came from the following:")
            print(district_dictionary_revenue[key]\
                    [\
                        (district_dictionary_revenue[key].amount\
                            == district_dictionary_revenue[key].amount.max())\
                    ]\
                .drop(columns=["last/business_name", "first_name"]))

            # Number of contributions of at least $500
            print(f"{district_dictionary_revenue[key]\
                    [\
                        (district_dictionary_revenue[key].amount >= 500)\
                    ]\
                .amount.count()\
                } contributions of at least $500 were made to the campaign.")
            
            # Ignore next steps if there are no contributions of 
            # at least $500:
            if district_dictionary_revenue[key]\
                [\
                    (district_dictionary_revenue[key].amount >= 500)\
                ].empty:
                next

            # Print all contributions of at least $500
            else:
                print(district_dictionary_revenue[key]\
                      [(district_dictionary_revenue[key].amount >= 500)]\
                    [["date", "payment_type", "amount", "first_name",
                    "last/business_name"]])
                print("")
            print(f"There were {district_dictionary_in_kind[key]\
                .amount.count()} in-kind contributions to {key}'s campaign.")
            print(f"In-kind contributions to {key}'s campaign totaled ${\
                district_dictionary_in_kind[key].amount.sum()}.")
            print()
            print("")
def write_a_district(
        house_or_senate: str,
        district: str | int,
        election: str,
        report: str,
        file_path: str
        ):
    """Create dictionaries containing the dataframes for each candidate in the 
    district. Then, write summaries of each of those candidates to a text file 
    with the specified filepath.

    Parameters
    ----------
    house_or_senate :
        String input to specify whether the district to write is a House or a
        Senate district. 
        Takes "house" or "senate" as inputs, all lowercase.
    district :
        The district to write.
        Input is a string with the Senate district in caps, such as "B",
        or the House district number, as a numeral, between 1 and 40.
    election :
        The election being reported on, as a string.
        For state races, probably either "State General" or "State Primary".
        If left blank, will write summaries for all elections.
    report :
        The report to summarize, as a string.
        For state races, probably either "Thirty Day" or "Seven Day".
        If left blank, will write summaries using all reports in the election.
    file_path :
        The path of the file to write summaries into. 
    """

    # Creates an empty dictionary for all transactions for each candidate in 
    # the district
    district_dictionary_all = {}

    # Creates an empty dictionary for only income transactions for each 
    # candidate in the district
    district_dictionary_revenue = {}

    district_dictionary_in_kind = {}

    # Creates an empty dictionary for only expense transactions for each 
    # candidate in the district
    district_dictionary_expenditure = {}

    with open(file_path, "a") as f:
        f.write("\n")
        if house_or_senate == "house":
            f.write(f"Summary for House District {district}:\n")

            # Defines a list of the candidates in the given House district, 
            # taken from the master list of lists
            district_candidates = nested_house_name_list[district-1]

            # Populates the "all transactions" dictionary with dataframes for 
            # each candidate 
            for candidate_name in district_candidates:
                district_dictionary_all[candidate_name] = \
                master_house_df_dictionary[candidate_name]\
                    [\
                        (master_house_df_dictionary[candidate_name]\
                            .report_type.str.contains(report))\
                        & (master_house_df_dictionary[candidate_name]\
                            .election_type.str.contains(election))\
                    ]
            
            # Populates the "revenue" dictionary with dataframes for each 
            # candidate 
            for candidate_name in district_candidates:
                district_dictionary_revenue[candidate_name] = \
                district_dictionary_all[candidate_name]\
                [\
                    (district_dictionary_all[candidate_name]\
                        .transaction_type == "Income")\
                    & (district_dictionary_all[candidate_name]\
                        .payment_type != "Non-Monetary")
                ]

            # Populates the "in-kind" dictionary with dataframes for each 
            # candidate 
            for candidate_name in district_candidates:
                district_dictionary_in_kind[candidate_name] = \
                district_dictionary_all[candidate_name]\
                    [\
                        (district_dictionary_all[candidate_name]\
                            .transaction_type == "Income")\
                        & (district_dictionary_all[candidate_name]\
                            .payment_type == "Non-Monetary")
                    ]

            # Populates the "all expenses" dictionary with dataframes for each 
            # candidate
            for candidate_name in nested_house_name_list[district-1]:
                district_dictionary_expenditure[candidate_name] = \
                district_dictionary_all[candidate_name]\
                    [\
                        district_dictionary_all[candidate_name]\
                        .transaction_type == "Expenditure"\
                    ]
            
        elif house_or_senate == "senate":
            f.write(f"Summary for Senate District {district}:\n")

            # Defines a list of the candidates in the given Senate district,
            # taken from the master list of lists
            district_candidates = nested_senate_name_list[\
                senate_districts.index(district)]

            # Populates the "all transactions" dictionary with dataframes for 
            # each candidate 
            for candidate_name in district_candidates:
                district_dictionary_all[candidate_name] = \
                master_senate_df_dictionary[candidate_name]\
                [\
                    (master_senate_df_dictionary[candidate_name]\
                        .report_type.str.contains(report))\
                    & (master_senate_df_dictionary[candidate_name]\
                        .election_type.str.contains(election))
                ]
            
            # Populates the "revenue" dictionary with dataframes for each 
            # candidate 
            for candidate_name in nested_senate_name_list[\
            senate_districts.index(district)]:
                district_dictionary_revenue[candidate_name] = \
                district_dictionary_all[candidate_name]\
                    [\
                        (district_dictionary_all[candidate_name]\
                            .transaction_type == "Income")\
                        & (district_dictionary_all[candidate_name]\
                            .payment_type != "Non-Monetary")
                    ]

            # Populates the "in-kind" dictionary with dataframes for each 
            # candidate
            for candidate_name in nested_senate_name_list[\
            senate_districts.index(district)]:
                district_dictionary_in_kind[candidate_name] = \
                    district_dictionary_all[candidate_name]\
                        [\
                            (district_dictionary_all[candidate_name]\
                                .transaction_type == "Income")\
                            & (district_dictionary_all[candidate_name]\
                                .payment_type == "Non-Monetary")\
                        ]

            # Populates the "all expenses" dictionary with dataframes for each 
            # candidate
            for candidate_name in nested_senate_name_list[\
            senate_districts.index(district)]:
                district_dictionary_expenditure[candidate_name] = \
                district_dictionary_all[candidate_name]\
                    [\
                        district_dictionary_all[candidate_name]\
                            .transaction_type == "Expenditure"\
                    ]
        
        # District header
        f.write("==================\n")
        f.write("\n")

        for key in district_dictionary_revenue.keys():
            
            # Candidate header
            f.write(f"{key}\n")
            f.write("----------------\n")

            # If there are no recorded transactions for the candidate:
            if district_dictionary_all[key].empty:
                f.write(f"There are no transactions recorded for {key}.\n")
                f.write("\n")
            else:
                # Total number of contributions
                if district_dictionary_revenue[key].empty:
                    f.write(f"{key}'s campaign did not record any donations.\n")
                else:
                    f.write(f"{key}'s campaign received {\
                        district_dictionary_revenue[key]\
                        .amount.count()} donations.\n")
                    
                    # Total revenue
                    f.write(f"Donations to {key}'s campaign totaled ${\
                                district_dictionary_revenue[key]\
                                .amount.sum()}.\n")
                    
                    # Average contribution
                    f.write(f"The average contribution to {\
                        key}'s campaign was ${\
                                round(district_dictionary_revenue[key]\
                                        .amount.mean(), 2)}.\n")
                    
                    # Median contribution
                    f.write(f"The median contribution to {key\
                            }'s campaign was ${\
                                district_dictionary_revenue[key]\
                                .amount.median()}.\n")
                    
                    # Minimum contribution
                    f.write(f"The minimum contribution to {key\
                            }'s campaign was ${\
                                district_dictionary_revenue[key]\
                                .amount.min()}.\n")
                    
                    # Maximum contribution
                    f.write(f"The maximum contribution to {key\
                            }'s campaign was ${\
                                district_dictionary_revenue[key]\
                                .amount.max()}.\n")
                    
                    # Number of contributions over $500
                    f.write(f"{district_dictionary_revenue[key]\
                            [\
                                (district_dictionary_revenue[key].amount >= 500) \
                                ].amount.count()\
                    } donations of at least $500 were made to the campaign.\n")
                    
                    # Sum of contributions exceeding $500
                    f.write(f"Donations of more than $500 totaled ${\
                        district_dictionary_revenue[key]\
                            [(district_dictionary_revenue[key].amount >= 500)]\
                        .amount.sum()} in the reporting period.\n")
                    
                f.write("\n")

                # In-kind contributions

                if district_dictionary_in_kind[key].amount.count() == 0:
                    f.write(f"{key}'s campaign received no in-kind contributions.\n")
                    f.write("\n")
                else:
                    # Total number of contributions
                    f.write(f"{key}'s campaign received {\
                        district_dictionary_in_kind[key]\
                        .amount.count()} in-kind contributions.\n")
                    
                    # Total revenue
                    f.write(f"In-kind contributions to {key}'s campaign totaled ${\
                                district_dictionary_in_kind[key]\
                                .amount.sum()}.\n")
                    
                    # Average contribution
                    f.write(f"The average in-kind contribution to {key}'s campaign"
                            f" had a value of ${round(\
                                district_dictionary_in_kind[key]\
                                .amount.mean(), 2)}.\n")
                    f.write("\n")


                # Expenditures

                if district_dictionary_expenditure[key].amount.count() == 0:
                    f.write(f"{key}'s campaign made no expenditures in the reporting period.\n")
                else:
                    # Total number of expenditures
                    f.write(f"{key}'s campaign made {\
                        district_dictionary_expenditure[key]\
                        .amount.count()} expenditures in the reporting period.\n")
                    
                    # Sum of campaign expenditures
                    f.write(f"{key}'s campaign spent ${\
                        district_dictionary_expenditure[key]\
                        .amount.sum()} in the reporting period.\n")
                    
                    # Average campaign expense
                    f.write(f"The average expense for {key}'s campaign was ${\
                        round((\
                            district_dictionary_expenditure[key]\
                            .amount.mean()\
                        ), 2)}.\n")
                    
                    # Median campaign expense
                    f.write(f"The median expense for {key}'s campaign was ${\
                        district_dictionary_expenditure[key]\
                        .amount.median()}.\n")
                    
                    # Minimum campaign expense
                    f.write(f"The smallest expense for {key}'s campaign was ${\
                        district_dictionary_expenditure[key]\
                        .amount.max()}.\n")
                    
                    # Maximum campaign expense
                    f.write(f"The biggest expense to {key}'s campaign was ${\
                        district_dictionary_expenditure[key]\
                        .amount.min()}.\n")
                    
                    # Number of expenses of at least $1,000
                    f.write(f"{district_dictionary_expenditure[key]\
                            [\
                                (district_dictionary_expenditure[key]\
                                    .amount <= -1000) \
                                ].amount.count()\
                    } expenses of at least $1,000 were made by the campaign.\n")
                    
                    # Total value of expenses of at least $1,000
                    f.write(f"Expenses exceeding $1,000 totaled ${\
                        district_dictionary_expenditure[key]\
                            [\
                                (district_dictionary_expenditure[key]\
                                    .amount <= -1000) \
                            ].amount.sum()\
                        } in the reporting period.\n")
                    
                f.write("\n")
                f.write("\n")

                """
                if district_dictionary_revenue[key]\
                    [\
                        district_dictionary_revenue[key]\
                        .amount >= 1000\
                    ].amount.count() == 0:
                    f.write("\n")
                else:
                    return (\
                        district_dictionary_revenue[key]\
                            [\
                                district_dictionary_revenue[key]\
                                .amount >= 1000\
                            ]\
                        [["date", "payment_type", "amount", "first_name", 
                        "last/business_name"]]\
                        )
                """

def summary_writer(
        election: str,
        report: str,
        file_path: str
        ):
    """Write summaries for all House districts and all Senate districts to
    the specified text file, using the given election and report.

    Parameters
    ----------
    election : 
        The election to summarize.
        For state races, probably either "Primary" or "General".
        If left blank, will summarize donations recorded during both the
        primary and the general election recording periods.
    report : 
        The report to use for summaries.
        For state races, probably either "Thirty Day" or "Seven Day".
        If left blank, will summarize all donations recorded for the given
        election, regardless of when they were reported. 
    file_path : 
        The name for the summary text file.

    """

    def house_summary():
        print("Attempting to write House candidate summaries...")
        house_write_start = time.time()
        for house_district in range(1, 41):
            write_a_district("house", house_district, election, report,
                            file_path)
        house_write_finish = time.time()
        print("All House candidate summaries successfully written.")
        print(f"Writing to file took {\
            round(house_write_finish - house_write_start, 5)} seconds.")
        print("")

    house_summary()

    def senate_summary():
        print("Attempting to write Senate candidate summaries...")
        senate_write_start = time.time()
        for senate_district in senate_districts:
            write_a_district("senate", senate_district, election, report, 
                             file_path)
        senate_write_finish = time.time()
        print("All Senate candidate summaries successfully written.")
        print(f"Writing to file took {\
            round(senate_write_finish - senate_write_start, 5)} seconds.")
        print("")

    senate_summary()
summary_writer(writing_election, writing_report, 
               "output_files/landfield_stuff/general_7_day_summaries.txt")



def big_donation_writer(election, report):
    """Write to a csv file all transactions in both the House and the Senate
    that were at least $1,000."""
    print("Attempting to write large House campaign donations to csv file...")
    csv_write_start = time.time()
    house_df[
        (house_df.amount >= 1000) 
        & (house_df.report_type.str.contains(report))
        & (house_df.election_type.str.contains(election))
        ]\
            [[
            "district", "candidate_name", "amount", "date", "donor_full_name",
            "address", "city", "state", "zip", "country", "employer", 
            "occupation", "donor_score", "is_self", "submitted"
            ]]\
        .sort_values(by=["district", "candidate_name", "amount"],
                     ascending = [True, True, False])\
        .to_csv("output_files/landfield_stuff/big_contributions.csv", 
                mode = "a", index=False)
    csv_write_finish = time.time()
    print("Large House donations successfully written.")
    print(f"Writing to file took {csv_write_finish - csv_write_start} \
          seconds.")
    print("")

    print("Attempting to write large Senate campaign donations to csv file...")
    csv_write_start = time.time()
    senate_df[
        (senate_df.amount >= 1000) 
        & (senate_df.report_type.str.contains(report))
        & (senate_df.election_type.str.contains(election))
        ]\
            [[
            "district", "candidate_name", "amount", "date", "donor_full_name",
            "address", "city", "state", "zip", "country", "employer", 
            "occupation", "donor_score", "is_self", "submitted"
            ]]\
        .sort_values(
            by=["district", "candidate_name", "amount"],
            ascending=[True, True, False])\
        .to_csv("output_files/big_contributions.csv",
                mode = "a", index=False, header=False)
    csv_write_finish = time.time()
    print("Large Senate donations successfully written.")
    print(f"Writing to file took {csv_write_finish - csv_write_start} \
          seconds.")
    print("")

def big_donation_iterator(
        district: int | str,
        election: str,
        report: str,
        file_path: str
        ):
    """For the given district, election and report type, writes to a csv file 
    all donations from entities whose donations totaled at least $500 across 
    the specified reporting period.
    
    Parameters
    ----------
    district : 
        The district to summarize.
        House districts are an integer, 1 through 40.
        Senate districts are an uppercase letter string, "A" through "T".
    election : 
        The election to summarize.
        For state races, probably either "Primary" or "General".
        If left blank, will summarize donations recorded during both the
        primary and the general election recording periods.
    report : 
        The report to use for summaries.
        For state races, probably either "Thirty Day" or "Seven Day".
        If left blank, will summarize all donations recorded for the given
        election, regardless of when they were reported.
    file_path : 
        The file path to write to.
        Generally, should look like
        "output_files/landfield_stuff/[name].csv" or
        "output_files/landfield_stuff/big_donations.csv"
    """
    
    # Creates an empty dictionary for all transactions for each candidate in 
    # the district
    district_dictionary_revenue = {}

    if isinstance(district, int):
        # Defines a list of the candidates in the given House district, 
        # taken from the master list of lists
        district_candidates = nested_house_name_list[district-1]

        # Populates the "all transactions" dictionary with dataframes for 
        # each candidate 
        for candidate_name in district_candidates:
            district_dictionary_revenue[candidate_name] = \
            master_house_df_dictionary[candidate_name]\
                [
                    (master_house_df_dictionary[candidate_name]\
                        .report_type.str.contains(report))
                    & (master_house_df_dictionary[candidate_name]\
                        .election_type.str.contains(election))
                    & (master_house_df_dictionary[candidate_name]\
                    .transaction_type == "Income")
                    & (master_house_df_dictionary[candidate_name]\
                    .payment_type != "Non-Monetary")
                ]\
                    [[
                    "district", "candidate_name", "amount", "date", 
                    "donor_full_name", "is_self", "address", "city", "state",
                    "zip", "country", "employer", "occupation", "payment_type",
                    "payment_detail", "purpose_of_expenditure", "submitted"
                    ]]
        i = 0
        for key in district_dictionary_revenue.keys():
            grouped_names = district_dictionary_revenue[key].groupby(\
                ["donor_full_name"]).amount.sum()
            
            # sugar_names is the list of donors who gave at least $500
            sugar_names = list(grouped_names[grouped_names >= 500].keys())
            
            # makes sure header is written only for the first candidate in
            # the first district. All other entries can use that header.
            if district == 1 and i == 0:
                district_dictionary_revenue[key]\
                    [
                        district_dictionary_revenue[key]\
                        .donor_full_name.isin(sugar_names)
                    ]\
                .sort_values(
                    by=["district", "candidate_name", "amount"],
                    ascending=[True, True, False])\
                .to_csv(file_path, mode = "a", index=False)
                i += 1
            else:
                district_dictionary_revenue[key]\
                    [
                        district_dictionary_revenue[key]\
                        .donor_full_name.isin(sugar_names)
                    ]\
                .sort_values(
                    by=["district", "candidate_name", "amount"],
                    ascending=[True, True, False])\
                .to_csv(file_path, mode = "a", index=False, header=None)
                i += 1
    elif isinstance(district, str):
        # Defines a list of the candidates in the given Senate district, 
        # taken from the master list of lists
        district_candidates = nested_senate_name_list[
            senate_districts.index(district)
            ]
        
        # Populates the "all transactions" dictionary with dataframes for 
        # each candidate 
        for candidate_name in district_candidates:
            district_dictionary_revenue[candidate_name] = \
            master_senate_df_dictionary[candidate_name]\
                [
                    (master_senate_df_dictionary[candidate_name]\
                        .report_type.str.contains(report))
                    & (master_senate_df_dictionary[candidate_name]\
                        .election_type.str.contains(election))
                    & (master_senate_df_dictionary[candidate_name]\
                    .transaction_type == "Income")
                    & (master_senate_df_dictionary[candidate_name]\
                    .payment_type != "Non-Monetary")
                ]\
                    [[
                    "district", "candidate_name", "amount", "date", 
                    "donor_full_name", "is_self", "address", "city", "state",
                    "zip", "country", "employer", "occupation", "payment_type",
                    "payment_detail", "purpose_of_expenditure", "submitted"
                    ]]
        i = 0
        for key in district_dictionary_revenue.keys():
            grouped_names = district_dictionary_revenue[key].groupby(\
                ["donor_full_name"]).amount.sum()
            
            # sugar_names is the list of donors who gave at least $500
            sugar_names = list(grouped_names[grouped_names >= 500].keys())
            
            # makes sure header is written only for the first candidate in
            # the first district. All other entries can use that header.
            district_dictionary_revenue[key]\
                [
                    district_dictionary_revenue[key]\
                    .donor_full_name.isin(sugar_names)
                ]\
            .sort_values(
                by=["district", "candidate_name", "amount"],
                ascending=[True, True, False])\
            .to_csv(file_path, mode = "a", index=False, header=None)
            i += 1
def aggregate_big_donation_iterator(file_path: str):
    house_district_iterator = range(1, 41)
    for district in house_district_iterator:
        big_donation_iterator(district, writing_election, writing_report, 
                              file_path)
    for senate_seat in senate_districts:
        big_donation_iterator(senate_seat, writing_election, writing_report, 
                              file_path)
aggregate_big_donation_iterator(
    "output_files/landfield_stuff/7_day_gen_big_donations.csv")

def big_expense_iterator(
        district: int | str,
        election: str,
        report: str,
        file_path: str
        ):
    """For a given district, election and report, writes to a .csv file all 
    expenses to entities who were paid at least $1,000 in total by the campaign
    across any number of transactions during the specified reporting period.
    
    Parameters
    ----------
    district : 
        The district to summarize.
        House districts are an integer, 1 through 40.
        Senate districts are an uppercase letter string, "A" through "T".
    election : 
        The election to summarize.
        For state races, probably either "Primary" or "General".
        If left blank, will summarize donations recorded during both the
        primary and the general election recording periods.
    report : 
        The report to use for summaries.
        For state races, probably either "Thirty Day" or "Seven Day".
        If left blank, will summarize all donations recorded for the given
        election, regardless of when they were reported.
    file_path : 
        The file path to write to.
        Generally, should start with the format 
        "output_files/landfield_stuff/[name].csv" or
        "output_files/landfield_stuff/big_expenditures.csv"
    """
    
    # This will hold all expenditures for each candidate in the district
    district_dictionary_expenditure = {}

    # Checks whether the "district" parameter is an integer, and thus whether
    # to get House candidate names.  
    if isinstance(district, int):
        # Defines a list of the candidates in the given House district, 
        # taken from the master list of lists
        district_candidates = nested_house_name_list[district-1]

        # Populates the "all transactions" dictionary with dataframes for 
        # each candidate 
        for candidate_name in district_candidates:
            district_dictionary_expenditure[candidate_name] = \
            master_house_df_dictionary[candidate_name]\
                [
                    (master_house_df_dictionary[candidate_name]\
                        .report_type.str.contains(report))
                    & (master_house_df_dictionary[candidate_name]\
                        .election_type.str.contains(election))
                    & (master_house_df_dictionary[candidate_name]\
                    .transaction_type == "Expenditure")
                ]\
                    [[
                    "district", "candidate_name", "amount", "date", 
                    "donor_full_name", "is_self", "address", "city", "state",
                    "zip", "country", "employer", "occupation", "payment_type",
                    "payment_detail", "purpose_of_expenditure", "submitted"
                    ]]
        i = 0
        for key in district_dictionary_expenditure.keys():
            grouped_names = district_dictionary_expenditure[key].groupby(\
                ["donor_full_name"]).amount.sum()
            
            # spend_names is the list of donors who gave at least $500
            spend_names = list(grouped_names[grouped_names <= -1000].keys())
            
            # Makes sure header is written only for the first candidate in
            # the first district. All other entries can use that header.
            if district == 1 and i == 0:
                district_dictionary_expenditure[key]\
                    [
                        district_dictionary_expenditure[key]\
                        .donor_full_name.isin(spend_names)
                    ]\
                .sort_values(
                    by=["district", "candidate_name", "amount"],
                    ascending=[True, True, True])\
                .to_csv(file_path, mode = "a", index=False)
                i += 1
            else:
                district_dictionary_expenditure[key]\
                    [
                        district_dictionary_expenditure[key]\
                        .donor_full_name.isin(spend_names)
                    ]\
                .sort_values(
                    by=["district", "candidate_name", "amount"],
                    ascending=[True, True, True])\
                .to_csv(file_path, mode = "a", index=False, header=None)
                i += 1
   
    # Checks whether the "district" parameter is a string, and thus whether
    # to get Senate candidate names. 
    elif isinstance(district, str):
        # Defines a list of the candidates in the given Senate district, 
        # taken from the master list of lists
        district_candidates = nested_senate_name_list[
            senate_districts.index(district)
            ]
        
        # Populates the "all transactions" dictionary with dataframes for 
        # each candidate 
        for candidate_name in district_candidates:
            district_dictionary_expenditure[candidate_name] = \
            master_senate_df_dictionary[candidate_name]\
                [
                    (master_senate_df_dictionary[candidate_name]\
                        .report_type.str.contains(report))
                    & (master_senate_df_dictionary[candidate_name]\
                        .election_type.str.contains(election))
                    & (master_senate_df_dictionary[candidate_name]\
                    .transaction_type == "Expenditure")
                ]\
                    [[
                    "district", "candidate_name", "amount", "date", 
                    "donor_full_name", "is_self", "address", "city", "state",
                    "zip", "country", "employer", "occupation", "payment_type",
                    "payment_detail", "purpose_of_expenditure", "submitted"
                    ]]
        i = 0
        for key in district_dictionary_expenditure.keys():
            grouped_names = district_dictionary_expenditure[key].groupby(\
                ["donor_full_name"]).amount.sum()
            
            # sugar_names is the list of donors who gave at least $500
            spend_names = list(grouped_names[grouped_names <= -1000].keys())
            
            # makes sure header is written only for the first candidate in
            # the first district. All other entries can use that header.
            district_dictionary_expenditure[key]\
                [
                    district_dictionary_expenditure[key]\
                    .donor_full_name.isin(spend_names)
                ]\
            .sort_values(
                by=["district", "candidate_name", "amount"],
                ascending=[True, True, True])\
            .to_csv(file_path, mode = "a", index=False, header=None)
            i += 1
def aggregate_big_expense_iterator(file_path: str):
    for district in range(1, 41):
        big_expense_iterator(district, writing_election, writing_report, 
                             file_path)
    for senate_seat in senate_districts:
        big_expense_iterator(senate_seat, writing_election, writing_report,
                            file_path)

aggregate_big_expense_iterator(
    "output_files/landfield_stuff/7_day_gen_big_expenses.csv")


print(big_df[(big_df.election_type == "State General") \
             & (big_df.report_type == "Thirty Day Report")]\
            .candidate_name.nunique())



general_house_list = [
    ["Jeremy T. Bynum", "Agnes C. Moran", "Grant EchoHawk"],
    ["Rebecca Himschoot"],
    ["Andrea \"Andi\" Story"],
    ["Sara Hannan"],
    ["Louise Stutes", "Leighton Radner"],
    ["Sarah L. Vance", "Brent Johnson", "Dawson Slaughter"],
    ["Justin Ruffridge", "Ronald D Gillham"],
    ["Bill Elam", "John Hillyer"],
    ["Lucy Bauer", "Ky Holland"],
    ["Craig W. Johnson", "Charles \"Chuck\" Kopp "],
    ["Julie Coulombe", "Walter Featherly"],
    ["Calvin R. Schrage", "Joseph Crisafi-Lurtsema"],
    ["Andrew Louis Josephson", "Heather Gottshall"],
    ["Alyse S. Galvin", "Harry Winner Kamdem"],
    ["Mia Costello", "Dustin T. Darden", "Denny Wells"],
    ["Carolyn Hall", "Nick Moe"],
    ["William Z. \"Zack\" Fields"],
    ["Cliff Groh", "David Nelson"],
    ["Genevieve Mina", "Kaylee M. Anderson", "Russell O. Wyatt"],
    ["Andrew T. Gray", "Scott A. Kohlhaas"],
    ["Donna C Mears", "Aimee Sims"],
    ["Stanley Wright", "Ted J. Eischeid"],
    ["Jamie Allard", "Jim Arlington"],
    ["Dan Saddler"],
    ["DeLena M Johnson"],
    ["Cathy Tilton"],
    ["David Eastman", "Jubilee Underwood"],
    ["Steve Menard", "Elexie Moore", "Wright, Jessica"],
    ["George Rauscher"],
    ["Kevin J. McCabe", "Doyle Holmes"],
    ["Maxine Dibert", "Barton S. LeBon"],
    ["Will Stapp", "Gary K. Damron"],
    ["Mike Prax"],
    ["Frank Tomaszewski", "Joy 'Joy Beth' Cottle"],
    ["Ashley Carrick", "Ruben A. McNeill Jr."],
    ["James Fields", "Pamela Goode", "Brandon P. Kowalski \"Putuuqti\"", 
        "Dana Mock", "Rebecca (Becky) Schwanke", "Mike Cronk"],
    ["Bryce Edgmon", "Darren Morgan Deacon"],
    ["CJ McCormick", "Nellie Darlene Jimmie", "Willy Keppel", 
        "Victoria Beatrice Sosa"],
    ["Neal Winston Foster", "Tyler Ivanoff"],
    ["Thomas C Ikaaq Baker", "Robyn Niayuq Burke", "Saima Chase"]
]
general_senate_list = [
    [],
    ["Jesse Kiehl"],
    [],
    ["Jesse J Bjorkman", "Ben Carpenter", "Tina Wegener"],
    [],
    ["Harold Borbridge", "James Kaufman", "Janice L. Park"],
    [],
    ["Matt Claman", "Liz Vazquez "],
    [],
    ["Forrest Dunbar", "Cheronda L. Smith"],
    [],
    ["Kelly R. Merrick", "Jared David Goecker", "Lee E Hammermeister"],
    [],
    ["David S. Wilson", "Wright, Stephen", "Robert D Yundt II"],
    [],
    ["Leslie Hajdukovich", "Scott Kawasaki"],
    [],
    ["Click Bishop", "Mike Cronk", "Savannah Fletcher", 
     "Williams, Robert 'Bert'"],
    [],
    ["Donald \"Donny\" C. Olson"]
]



#pick_a_district("house", 12, "", "")


# DF.INSERT() SHOULD BE THE ANSWER TO MOVING NEW COLUMNS TO NEW POSITIONS 
# WITHOUT REWRITING EVERYTHING

#ortiz_df = big_df[big_df.candidate_name == "Daniel (Dan) Ortiz"]
#print(ortiz_df.head(10))
#print(ortiz_df[ortiz_df.transaction_type == "Income"].amount.sum())