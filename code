import pandas as pd

# Load data from Excel file
excel_file = 'Supply chain logisitcs problem.xlsx'

# Read data from Excel sheets into Pandas DataFrames
df_wh_capacities = pd.read_excel(excel_file, 'WhCapacities')
df_products_per_plant = pd.read_excel(excel_file, 'ProductsPerPlant')
df_wh_costs = pd.read_excel(excel_file, 'WhCosts')
df_plant_ports = pd.read_excel(excel_file, 'PlantPorts')
df_vmi_customers = pd.read_excel(excel_file, 'VmiCustomers')

# Check for potential issues in plant-port connections
issues = []

# Check for duplicate entries in the 'plant ports' table
duplicate_entries = df_plant_ports[df_plant_ports.duplicated(subset=['Plant Code', 'Port'], keep=False)]
if not duplicate_entries.empty:
    issues.append(('Duplicate Entries', duplicate_entries))

# Check for missing or null values in 'Plant Code' or 'Port' columns
missing_values = df_plant_ports[df_plant_ports['Plant Code'].isnull() | df_plant_ports['Port'].isnull()]
if not missing_values.empty:
    issues.append(('Missing Values', missing_values))

# Check for orphaned ports (ports without any associated plant)
orphaned_ports = df_plant_ports[~df_plant_ports['Port'].isin(df_wh_capacities['Plant ID'].unique())]
if not orphaned_ports.empty:
    issues.append(('Orphaned Ports', orphaned_ports))

# Check for orphaned plants (plants without any associated port)
orphaned_plants = df_wh_capacities[~df_wh_capacities['Plant ID'].isin(df_plant_ports['Plant Code'].unique())]
if not orphaned_plants.empty:
    issues.append(('Orphaned Plants', orphaned_plants))

# Display any identified issues in plant-port connections
if issues:
    print("Potential issues in plant-port connections:")
    for issue_type, data in issues:
        print(f"\n{issue_type}:")
        print(data)
else:
    print("No issues found in plant-port connections.")

# Check for potential threats related to warehouse capacities and product utilization
merged_capacity_usage = pd.merge(df_wh_capacities, df_products_per_plant, on='Plant ID', how='left')
merged_capacity_usage['Utilization'] = merged_capacity_usage['Product ID'].fillna(0)
merged_capacity_usage['Excess Capacity'] = merged_capacity_usage['Daily Capacity'] - merged_capacity_usage['Utilization']
excess_capacity_threshold = 50  # Threshold for excess capacity (customize as needed)
potential_risks_capacity = merged_capacity_usage[merged_capacity_usage['Excess Capacity'] > excess_capacity_threshold]

if not potential_risks_capacity.empty:
    print("Warehouses with potential excess capacity:")
    print(potential_risks_capacity[['Plant ID', 'Daily Capacity', 'Utilization', 'Excess Capacity']])
else:
    print("No warehouses found with potential excess capacity.")

# Evaluate anomalies in cost per unit across plants
mean_cost_per_unit = df_wh_costs.groupby('WH')['Cost/unit'].mean()
std_cost_per_unit = df_wh_costs.groupby('WH')['Cost/unit'].std()
anomaly_threshold = 2  # Threshold for detecting anomalies based on standard deviation

anomalies_cost_per_unit = []
for wh, mean, std in zip(mean_cost_per_unit.index, mean_cost_per_unit, std_cost_per_unit):
    upper_limit = mean + anomaly_threshold * std
    lower_limit = mean - anomaly_threshold * std
    wh_data = df_wh_costs[df_wh_costs['WH'] == wh]
    wh_anomalies = wh_data[(wh_data['Cost/unit'] > upper_limit) | (wh_data['Cost/unit'] < lower_limit)]
    if not wh_anomalies.empty:
        anomalies_cost_per_unit.append((wh, wh_anomalies))

if anomalies_cost_per_unit:
    print("\nWarehouses with potential cost per unit anomalies:")
    for wh, anomalies in anomalies_cost_per_unit:
        print(f"\nWarehouse: {wh}")
        print(anomalies)
else:
    print("\nNo anomalies found in cost per unit across warehouses.")

# Perform additional threat identification checks based on your scenario and data
# This might include checking VMI customers' data, further analysis of plant-port connections, etc.
# Adjust and add more checks based on your understanding of the data and potential risks in the supply chain logistics context.
# For instance, you might check for inconsistencies in VMI customers' data or unauthorized plant-port connections.

# Example: Check for inconsistencies or mismatches in VMI customers' data
vmi_customers_issues = []
unique_customers = df_vmi_customers['Customers'].unique()
unique_plants = df_wh_capacities['Plant ID'].unique()

for customer in unique_customers:
    if customer not in unique_plants:
        vmi_customers_issues.append(customer)

if vmi_customers_issues:
    print("Potential issues with VMI customers' data:")
    print(vmi_customers_issues)
else:
    print("No issues found in VMI customers' data.")

# Example: Identify potential unauthorized plant-port connections
authorized_connections = df_plant_ports[['Plant Code', 'Port']].values.tolist()
expected_connections = [(row['Plant Code'], row['Port']) for _, row in df_plant_ports.iterrows()]

suspicious_connections = []

for index, row in df_plant_ports.iterrows():
    connection = (row['Plant Code'], row['Port'])
    if connection not in authorized_connections:
        suspicious_connections.append(connection)

if suspicious_connections:
    print("\nPotential unauthorized plant-port connections:")
    print(suspicious_connections)
else:
    print("\nAll plant-port connections are as expected.")

# Save identified threats and conclusions to a file or take necessary actions as per the organization's protocols.

# Note: We can adjust, modify, or expand this code according to our specific data structure, context, and the types of threats that we want to identify in our supply chain logistics scenario.



identified_threats = [
    {
        'Type': 'Plant-Port Connection Issues',
        'Details': issues
    },
    {
        'Type': 'Warehouses with Potential Excess Capacity',
        'Details': potential_risks_capacity[['Plant Code', 'Daily Capacity', 'Utilization', 'Excess Capacity']].to_dict(orient='records')
    },
    {
        'Type': 'Plants with Potential Cost per Unit Anomalies',
        'Details': {plant: anomalies.to_dict(orient='records') for plant, anomalies in anomalies_cost_per_unit}
    },
    {
        'Type': 'Inconsistencies in VMI Customers Data',
        'Details': vmi_customers_issues
    },
    {
        'Type': 'Unauthorized Plant-Port Connections',
        'Details': suspicious_connections
    }
]

# Conclusions
conclusion = "Based on the analysis, several potential threats and discrepancies have been identified in the supply chain data, including plant-port connection discrepancies, excess warehouse capacity, anomalies in cost per unit, inconsistencies in VMI customers' data, and potential unauthorized plant-port connections. Further investigation and mitigation steps are recommended to address these issues and enhance supply chain security."

# Save identified threats and conclusions to a new file
output_file = 'identified_threats_and_conclusions.txt'

with open(output_file, 'w') as file:
    file.write("Identified Threats:\n\n")
    for threat in identified_threats:
        file.write(f"{threat['Type']}:\n")
        if isinstance(threat['Details'], list):
            for detail in threat['Details']:
                file.write(f"{detail}\n")
        else:
            file.write(f"{threat['Details']}\n")
        file.write("\n")

    file.write("\nConclusions:\n")
    file.write(conclusion)

print(f"Identified threats and conclusions have been saved to {conclusion }.")
