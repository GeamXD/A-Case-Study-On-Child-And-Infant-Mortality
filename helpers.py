import pandas as pd
def missing_data(data):
    total = data.isnull().sum().sort_values(ascending = False)
    percent = (data.isnull().sum()/data.isnull().count()*100).sort_values(ascending = False)
    return pd.concat([total, percent], axis=1, keys=['Total', 'Percent'])

def wrangle_health_data(data_directory='data/'):
    """
    Wrangle and merge health-related datasets.

    Parameters:
    -----------
    data_directory : str, optional
        Path to the directory containing CSV files (default is 'data/')

    Returns:
    --------
    pd.DataFrame
        Merged and cleaned dataset
    """
    # Define file paths
    files = {
        'health_protection': 'health-protection-coverage.csv',
        'vaccination_coverage': 'global-vaccination-coverage.csv',
        'births_attended': 'births-attended-by-health-staff-sdgs.csv',
        'maternal_deaths': 'number-of-maternal-deaths-by-region.csv',
        'child_mortality': 'child-mortality-by-income-level-of-country.csv',
        'infant_deaths': 'number-of-infant-deaths-unwpp.csv',
        'youth_mortality': 'youth-mortality-rate.csv',
        'causes_of_death': 'Distribution of Causes of Death among Children Aged less than 5 years.csv'
    }

    # Load datasets
    datasets = {}
    for key, filename in files.items():
        filepath = f"{data_directory.rstrip('/')}/{filename}"
        datasets[key] = pd.read_csv(filepath)
        print(f"Loaded {key}: {len(datasets[key])} rows")

    # Clean vaccination_coverage
    datasets['vaccination_coverage'] = datasets['vaccination_coverage'].dropna(subset=['Code'])

    # Clean maternal_deaths
    if '959828-annotations' in datasets['maternal_deaths'].columns:
        datasets['maternal_deaths'].drop(columns=['959828-annotations'], inplace=True)

    # Clean causes_of_death
    cols_to_drop = [
        'DataSourceDimValueCode', 'Dim3', 'DataSource', 'Dim3 type',
        'Dim3ValueCode', 'FactComments', 'FactValueNumericHigh',
        'FactValueNumericHighPrefix', 'FactValueNumericLow',
        'FactValueNumericLowPrefix', 'FactValueNumericPrefix',
        'FactValueTranslationID', 'FactValueUoM'
    ]

    # Drop specified columns in causes_of_death
    datasets['causes_of_death'].drop(columns=cols_to_drop, inplace=True)

    # Ensure all datasets have no missing values in 'Code'
    for key in ['vaccination_coverage', 'births_attended', 'maternal_deaths',
                'child_mortality', 'infant_deaths', 'youth_mortality']:
        datasets[key] = datasets[key].dropna(subset=['Code'])

    # Merge datasets
    # Start with health_protection and vaccination_coverage
    merged_data = datasets['health_protection'].merge(
        datasets['vaccination_coverage'],
        on=['Code', 'Year'],
        how='inner',
        suffixes=('_health', '_vacc')
    )

    # Subsequent merges
    merge_order = [
        'births_attended',
        'maternal_deaths',
        'child_mortality',
        'infant_deaths',
        'youth_mortality'
    ]

    for dataset_key in merge_order:
        merged_data = merged_data.merge(
            datasets[dataset_key],
            on=['Code', 'Year'],
            how='inner',
            suffixes=('', f'_{dataset_key}')
        )

    # Final merge with causes of death
    merged_data = pd.merge(
        merged_data,
        datasets['causes_of_death'],
        left_on=['Code', 'Year'],
        right_on=['SpatialDimValueCode', 'Period'],
        how='inner'
    )

    # Print merge stats
    print("\nMerge Statistics:")
    print(f"Final merged dataset: {len(merged_data)} rows")

    # Drop columns that are not needed for the analysis
    drop_cols = ['IndicatorCode', 'Indicator', 'ValueType', 'ParentLocationCode',
             'Location type', 'SpatialDimValueCode',
             'Period type', 'Period', 'IsLatestYear', 'Dim1 type',
             'Dim1ValueCode', 'Dim2 type', 'Dim2ValueCode', 'Language', 'DateModified',
             'Entity_health', 'Entity_vacc', 'Entity_child', 'Entity_births', 'Entity_infant', 'Entity_youth', 'Location',]
    merged_data.drop(columns=drop_cols, inplace=True)

    # Truncate some column names for readability
    rename_cols = ['Share of population covered by health insurance (ILO (2014))','Observation value - Indicator: Under-five mortality rate - Sex: Total - Wealth quintile: Total - Unit of measure: Deaths per 100 live births']
    merged_data.columns = merged_data.columns.str.replace('Share of population covered by health insurance (ILO (2014))', 'Health insurance coverage (ILO, 2014)')
    merged_data.columns = merged_data.columns.str.replace('Observation value - Indicator: Under-five mortality rate - Sex: Total - Wealth quintile: Total - Unit of measure: Deaths per 100 live births', 'Under-five mortality rate - Total - Deaths per 100 live births')

    return merged_data