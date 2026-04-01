#%%
import yaml
import os

# Output folder
output_folder = r"C:\UVI\QAQC stuff\src\metadater"
os.makedirs(output_folder, exist_ok=True)

# Site-specific global info
site_modifications = {
    "TCSAVA": {
        "title": "Benthic temperature record from Savana, St. Thomas USVI",
        "location": "Savana",
        "depth": "9 m",
        "site_description": "Fringing coral reef near uninhabited Savana Island",
        "coordinates": "18.34064,",
        "geospatial_lat_max": "18.34064,-65.08205",
        "geospatial_lat_min": "18.34064",
        "geospatial_lon_max": "-65.08205",
        "geospatial_lon_min": "-65.08205"
    },
    "TCSHCS": {
        "title": "Benthic temperature record from Seahorse Cottage Shoal, St. Thomas, USVI",
        "location": "Seahorse Cottage Shoal",
        "depth": "20 m",
        "site_description": "Isoloated midshelf orbicellid bank reef with high coral cover",
        "coordinates": "18.29467,-64.8675",
        "geospatial_lat_max": "18.29467",
        "geospatial_lat_min": "18.29467",
        "geospatial_lon_max": "-64.8675",
        "geospatial_lon_min": "-64.8675"
    },
    "TCSCAP": {
        "title": " Benthic temperature record from South Capella, St. Thomas, USVI",
        "location": "South Capella",
        "depth": "20 m",
        "site_description": "Midshelf orbicellid linear reef with high coral cover",
        "coordinates": "18.26267, -64.87237",
        "geospatial_lat_max": "18.26267",
        "geospatial_lat_min": "18.26267",
        "geospatial_lon_max": "-64.87237",
        "geospatial_lon_min": "-64.87237"
    },
    "TCSC35": {
        "title": "Benthic temperature record from South Capella, St. Thomas, USVI",
        "location": "South Capella 35m",
        "depth": "35 m",
        "site_description": "Temperature probe only site",
        "coordinates": "18.26267,-64.87237",
        "geospatial_lat_max": "18.26267",
        "geospatial_lat_min": "18.26267",
        "geospatial_lon_max": "-64.87237",
        "geospatial_lon_min": "-64.87237"
    },
    "TCSWAT": {
        "title": "Benthic temperature record from South Water, St. Croix USVI",
        "location": "South Water",
        "depth": "20 m",
        "site_description": "Midshelf hardbottom site with diverse fish community",
        "coordinates": "18.28068,-64.94592",
        "geospatial_lat_max": "18.28068",
        "geospatial_lat_min": "18.28068",
        "geospatial_lon_max": "-64.94592",
        "geospatial_lon_min": "-64.94592"
    },
    "TCLSTJ": {
        "title": "Benthic temperature record from St James, St. Thomas, USVI",
        "location": "St James",
        "depth": "15 m",
        "site_description": "Patch reef near developing cay",
        "coordinates": "18.29459, -64.83238",
        "geospatial_lat_max": "18.29459",
        "geospatial_lat_min": "18.29459",
        "geospatial_lon_max": "-64.83238",
        "geospatial_lon_min": "-64.83238"
    },
    "TCBKIX": {
        "title": "Benthic temperature record from Buck Island, St. Croix, USVI",
        "location": "Buck Island",
        "depth": "15 m",
        "site_description": "Orbicella reef southeast of the Buck Island National Park barrier reef within the Virgin Islands National Monument",
        "coordinates": "17.78500, -64.60917",
        "geospatial_lat_max": "17.78500",
        "geospatial_lat_min": "17.78500",
        "geospatial_lon_max": "-64.60917",
        "geospatial_lon_min": "-64.60917"
    },
    "TCBX33": {
        "title": "Benthic temperature record from Buck Island, St. Croix, USVI",
        "location": "Buck Island",
        "depth": "33 m",
        "site_description": "Mesophotic reef with high orbicellid cover north of Buck Island within the Virgin Islands National Monument",
        "coordinates": "17.80659, -64.59935",
        "geospatial_lat_max": "17.80659",
        "geospatial_lat_min": "17.80659",
        "geospatial_lon_max": "-64.59935",
        "geospatial_lon_min": "-64.59935"
    },
    "TCCB08": {
        "title": "Benthic temperature record from Cane Bay, St. Croix, USVI",
        "location": "Cane Bay",
        "depth": "10 m",
        "site_description": "Orbicella reef near a dive mooring",
        "coordinates": "17.77388, -64.81350",
        "geospatial_lat_max": "17.77388",
        "geospatial_lat_min": "17.77388",
        "geospatial_lon_max": "-64.81350",
        "geospatial_lon_min": "-64.81350"
    },
    "TCCB40": {
        "title": "Benthic temperature record from Cane Bay Deep, St. Croix, USVI",
        "location": "Cane Bay Deep",
        "depth": "38 m",
        "site_description": "Wall reef closest offshelf from Cane Bay shallow site",
        "coordinates": "17.77661, -64.81522",
        "geospatial_lat_max": "17.77661",
        "geospatial_lat_min": "17.77661",
        "geospatial_lon_max": "-64.81522",
        "geospatial_lon_min": "-64.81522"
    },
    "TCCB99": {
        "title": "Benthic temperature record from Cane Bay, St. Croix, USVI",
        "location": "Cane Bay",
        "depth": "100 m",
        "site_description": "New site description for TCCB99",
        "coordinates": "17.77661, ",
        "geospatial_lat_max": "17.77661",
        "geospatial_lat_min": "17.77661",
        "geospatial_lon_max": "-64.81522",
        "geospatial_lon_min": "-64.81522"
    },
    "TCCB67": {
        "title": "Benthic temperature record from Cane Bay, St. Croix, USVI",
        "location": "Cane Bay",
        "depth": "67 m",
        "site_description": "New site description for TCCB67",
        "coordinates": "17.77661",
        "geospatial_lat_max": "17.77661",
        "geospatial_lat_min": "17.77661",
        "geospatial_lon_max": "-64.81522",
        "geospatial_lon_min": "-64.81522"
    },
    "TCCSTL": {
        "title": "Benthic temperature record from Castle, St. Croix, USVI",
        "location": "Castle",
        "depth": "7 m",
        "site_description": "Haphazard selection along the Tague Bay reef near the closed West Indies Laboratory",
        "coordinates": "17.76278, -64.59743",
        "geospatial_lat_max": "17.76278",
        "geospatial_lat_min": "17.76278",
        "geospatial_lon_max": "-64.59743",
        "geospatial_lon_min": "-64.59743"
    },
    "TCEAGR": {
        "title": "Benthic temperature record from Eagle Ray, St. Croix, USVI",
        "location": "Eagle Ray",
        "depth": "10 m",
        "site_description": "Dive mooring near mouth of Christiansted Harbor",
        "coordinates": "17.7615, -64.6988",
        "geospatial_lat_max": "17.7615",
        "geospatial_lat_min": "17.7615",
        "geospatial_lon_max": "-64.6988",
        "geospatial_lon_min": "-64.6988"
    },
    "TCGRPD": {
        "title": "Benthic temperature record from Great Pond, St. Croix, USVI",
        "location": "Great Pond",
        "depth": "6 m",
        "site_description": "Highest presence of Acropora on south shore of St. Croix in the East End Marine Park",
        "coordinates": "17.71097, -64.65221",
        "geospatial_lat_max": "17.71097",
        "geospatial_lat_min": "17.71097",
        "geospatial_lon_max": "-64.65221",
        "geospatial_lon_min": "-64.65221"
    },
    "TCJCKB": {
        "title": "Benthic temperature record from Jacks Bay, St. Croix, USVI",
        "location": "Jacks Bay",
        "depth": "14 m",
        "site_description": "In East End Marine Park near the southeast tip of St. Croix",
        "coordinates": "17.74337, -64.57160",
        "geospatial_lat_max": "17.74337",
        "geospatial_lat_min": "17.74337",
        "geospatial_lon_max": "-64.57160",
        "geospatial_lon_min": "-64.57160"
    },
    'TCCORB': {
        'title': 'Benthic temperature record from Coral Bay, St. John, USVI',
        'location': 'Coral Bay',
        'depth': '9 m',
        'site_description': 'Patch reef in Coral Bay outside Coral Harbor',
        'coordinates': '18.33797, -64.70402',
        'geospatial_lat_max': '18.33797',
        'geospatial_lat_min': '18.33797',
        'geospatial_lon_max': '-64.70402',
        'geospatial_lon_min': '-64.70402',
    },

    # TCFSHB
    'TCFSHB': {
        'title': ' Benthic temperature record from Fish Bay, St. John, USVI',
        'location': 'Fish Bay',
        'depth': '6 m',
        'site_description': 'Just outside the Virgin Islands National Park on the western shore along a gradient of land based sources of pollution',
        'coordinates': '18.31417, -64.76408',
        'geospatial_lat_max': '18.31417',
        'geospatial_lat_min': '18.31417',
        'geospatial_lon_max': '-64.76408',
        'geospatial_lon_min': '-64.76408',
    },

    # TCMERI
    'TCMERI': {
        'title': 'Benthic temperature record from Meri Shoal, St. John, USVI',
        'location': 'Meri Shoal',
        'depth': '30 m',
        'site_description': 'Mesophotic reef off St. John south of Pilsbury Sound and near the CariCOOS Buoy VI1',
        'coordinates': '18.24447,-64.75862',
        'geospatial_lat_max': '18.24447',
        'geospatial_lat_min': '18.24447',
        'geospatial_lon_max': '-64.75862',
        'geospatial_lon_min': '-64.75862',
    },

    # TCBKPT
    'TCBKPT': {
        'title': 'Benthic temperature record from Black Point, St. Thomas, USVI',
        'location': 'Black Point',
        'depth': '9 m',
        'site_description': 'High cover mixed orbicellid fringing reef near UVI',
        'coordinates': '18.34450, -64.98595',
        'geospatial_lat_max': '18.34450',
        'geospatial_lat_min': '18.34450',
        'geospatial_lon_max': '-64.98595',
        'geospatial_lon_min': '-64.98595',
    },

    # TCBOTB
    'TCBOTB': {
        'title': 'Benthic temperature record from Botany Bay, St. Thomas, USVI',
        'location': 'Botany Bay',
        'depth': '8 m',
        'site_description': 'High cover reef near development (Botany Bay) on northside of St. Thomas',
        'coordinates': '18.35738, -65.03442',
        'geospatial_lat_max': '18.35738',
        'geospatial_lat_min': '18.35738',
        'geospatial_lon_max': '-65.03442',
        'geospatial_lon_min': '-65.03442',
    },

    # TCBRWB
    'TCBRWB': {
        'title': 'Benthic temperature record from Brewers Bay, St. Thomas, USVI',
        'location': 'Brewers Bay',
        'depth': '6 m',
        'site_description': 'High cover Orbicella annularis reef near UVI',
        'coordinates': '18.34403, -64.98435',
        'geospatial_lat_max': '18.34403',
        'geospatial_lat_min': '18.34403',
        'geospatial_lon_max': '-64.98435',
        'geospatial_lon_min': '-64.98435',
    },

    # TCBKIT
    'TCBKIT': {
        'title': 'Benthic temperature record from Buck Island, St. Thomas, USVI',
        'location': 'Buck Island',
        'depth': '14 m',
        'site_description': 'Fringing reef near north of uninhabited Buck Island, St. Thomas',
        'coordinates': '18.27883, -64.89833',
        'geospatial_lat_max': '18.27883',
        'geospatial_lat_min': '18.27883',
        'geospatial_lon_max': '-64.89833',
        'geospatial_lon_min': '-64.89833',
    },

    # TCCORK
    'TCCORK': {
        'title': 'Benthic temperature record from Coculus Rock, St. Thomas, USVI',
        'location': 'Coculus Rock',
        'depth': '7 m',
        'site_description': 'Fringing reef on basalt near mouth of Benner Bay and Mangrove Lagoon',
        'coordinates': '18.31257, -64.86058',
        'geospatial_lat_max': '18.31257',
        'geospatial_lat_min': '18.31257',
        'geospatial_lon_max': '-64.86058',
        'geospatial_lon_min': '-64.86058',
    },

    # TCCLGE
    'TCCLGE': {
        'title': 'Benthic temperature record from College Shoal East, St. Thomas, USVI',
        'location': 'College Shoal East',
        'depth': '30 m',
        'site_description': 'Mesophotic reef with high orbicellid cover inside the Hind Bank Marine Conservation District',
        'coordinates': '18.18568, -65.07677',
        'geospatial_lat_max': '18.18568',
        'geospatial_lat_min': '18.18568',
        'geospatial_lon_max': '-65.07677',
        'geospatial_lon_min': '-65.07677',
    },

    # TCFLTC
    'TCFLTC': {
        'title': 'Benthic temperature record from Flat Cay, St. Thomas, USVI',
        'location': 'Flat Cay',
        'depth': '12 m',
        'site_description': 'Fringing reef near uninhabited cay southwest of St. Thomas',
        'coordinates': '18.31822, -64.99104',
        'geospatial_lat_max': '18.31822',
        'geospatial_lat_min': '18.31822',
        'geospatial_lon_max': '-64.99104',
        'geospatial_lon_min': '-64.99104',
    },

    # TCGB63
    'TCGB63': {
        'title': 'Benthic temperature record from Ginsburg Fringe, St. Thomas, USVI',
        'location': 'Ginsburg Fringe',
        'depth': '63 m',
        'site_description': 'Lower mesophotic coral reef on well-developed agaricid fringe',
        'coordinates': '18.1877,-64.95998',
        'geospatial_lat_max': '18.1877',
        'geospatial_lat_min': '18.1877',
        'geospatial_lon_max': '-64.95998',
        'geospatial_lon_min': '-64.95998',
    },

    # TCGMKT
    'TCGMKT': {
        'title': 'Benthic temperature record from Grammanik Tiger, St. Thomas, USVI',
        'location': 'Grammanik Tiger',
        'depth': '38 m',
        'site_description': 'Mesophotic coral reef at multi-species fish spawning aggregation in the Grammanik Bank fisheries seasonal closed area',
        'coordinates': '18.18885, -64.95659',
        'geospatial_lat_max': '18.18885',
        'geospatial_lat_min': '18.18885',
        'geospatial_lon_max': '-64.95659',
        'geospatial_lon_min': '-64.95659',
    },

    # TCHB40
    'TCHB40': {
        'title': 'Benthic temperature record from Hind Bank, St. Thomas, USVI',
        'location': 'Hind Bank',
        'depth': '39 m',
        'site_description': 'Mesophotic coral reef at red hind (Epinephelus guttatus) fish spawning aggregation in the Hind Bank Marine Conservation District',
        'coordinates': '18.20217, -65.00158',
        'geospatial_lat_max': '18.20217',
        'geospatial_lat_min': '18.20217',
        'geospatial_lon_max': '-65.00158',
        'geospatial_lon_min': '-65.00158',
    },

    # TCHB30
    'TCHB30': {
        'title': 'Benthic temperature record from Hind Bank, St. Thomas, USVI',
        'location': 'Hind Bank',
        'depth': '30 m',
        'site_description': 'Thermistor string deployment at a minimum 30m depth.  Line swing can cause deepening of sensor.',
        'coordinates': '18.20217, -65.00158',
        'geospatial_lat_max': '18.20217',
        'geospatial_lat_min': '18.20217',
        'geospatial_lon_max': '-65.00158',
        'geospatial_lon_min': '-65.00158',
    },

    # TCHB20
    'TCHB20': {
        'title': 'Benthic temperature record from Hind Bank, St. Thomas, USVI',
        'location': 'Hind Bank',
        'depth': '20 m',
        'site_description': 'Thermistor string deployment at a minimum of 20m depth. Line swing can cause deepening of sensor.',
        'coordinates': '18.20217, -65.00158',
        'geospatial_lat_max': '18.20217',
        'geospatial_lat_min': '18.20217',
        'geospatial_lon_max': '-65.00158',
        'geospatial_lon_min': '-65.00158',
    },

    # TCMAGB
    'TCMAGB': {
        'source': 'Benthic temperature record from Magens Bay, St. Thomas, USVI ',
        'location': 'Magens Bay',
        'depth': '7 m',
        'site_description': 'Fringing coral reef on northside of St. Thomas impacted by sedimentation from developed hillsides of the watershed',
        'coordinates': '18.37425, -64.93438',
        'geospatial_lat_max': '18.37425',
        'geospatial_lat_min': '18.37425',
        'geospatial_lon_max': '-64.93438',
        'geospatial_lon_min': '-64.93438',
    },
    "TCKNGC": {
        "title": "Benthic temperature record from Kings Corner, St. Croix, USVI",
        "location": "Kings Corner",
        "depth": "17 m",
        "site_description": "Western St. Croix site south of Fredriksted",
        "coordinates": "17.69116, -64.90008",
        "geospatial_lat_max": "17.69116",
        "geospatial_lat_min": "17.69116",
        "geospatial_lon_max": "-64.90008",
        "geospatial_lon_min": "-64.90008"
    },
    "TCLBEM": {
        "title": "Benthic temperature record from Lang Bank EEMP, St. Croix, USVI",
        "location": "Lang Bank EEMP",
        "depth": "27 m",
        "site_description": "Mesophotic coral reef in EEMP.  Selected haphazardly",
        "coordinates": "17.72145, -64.54706",
        "geospatial_lat_max": "17.72145",
        "geospatial_lat_min": "17.72145",
        "geospatial_lon_max": "-64.54706",
        "geospatial_lon_min": "-64.54706"
    },
    "TCLB99": {
        "title": "Benthic temperature record from Lang Bank, St. Croix, USVI",
        "location": "Lang Bank",
        "depth": "100 m",
        "site_description": "New site description for TCLB99",
        "coordinates": "17.72145",
        "geospatial_lat_max": "17.72145",
        "geospatial_lat_min": "17.72145",
        "geospatial_lon_max": "-64.54706",
        "geospatial_lon_min": "-64.54706"
    },
    "TCLB67": {
        "title": "Benthic temperature record from Lang Bank, St. Croix, USVI",
        "location": "Lang Bank",
        "depth": "67 m",
        "site_description": "New site description for TCLB67",
        "coordinates": "17.72145, -64.54706",
        "geospatial_lat_max": "17.72145",
        "geospatial_lat_min": "17.72145",
        "geospatial_lon_max": "-64.54706",
        "geospatial_lon_min": "-64.54706"
    },
    "TCLBRH": {
        "title": "Benthic temperature record from Lang Bank Red Hind FSA, St. Croix, USVI ",
        "location": "Land Bank Red Hind FSA",
        "depth": "33 m",
        "site_description": "Colocated with a Fish Spawning Aggregation of red hind (Epinephelus guttatus)",
        "coordinates": "17.82427, -64.44963",
        "geospatial_lat_max": "17.82427",
        "geospatial_lat_min": "17.82427",
        "geospatial_lon_max": "-64.44963",
        "geospatial_lon_min": "-64.44963"
    },
    "TCMT24": {
        "title": "Benthic temperature record from Mutton, St. Croix, USVI",
        "location": "Mutton Snapper FSA",
        "depth": "24 m",
        "site_description": "Colocation with closed area protecting spawning staging area of muton snapper (Lutjanus analis)",
        "coordinates": "17.6366,-64.8624",
        "geospatial_lat_max": "17.6366",
        "geospatial_lat_min": "17.6366",
        "geospatial_lon_max": "-64.8624",
        "geospatial_lon_min": "-64.8624"
    },
    "TCMT40": {
        "title": "Benthic temperature record from Mutton, St. Croix, USVI",
        "location": "Mutton Snapper FSA 40m",
        "depth": "40 m",
        "site_description": "Mesophotic temperature monitoring location just offshelf from Mutton Snapper Site.",
        "coordinates": "17.6366,-64.8624",
        "geospatial_lat_max": "17.6366",
        "geospatial_lat_min": "17.6366",
        "geospatial_lon_max": "-64.8624",
        "geospatial_lon_min": "-64.8624"
    },
    "TCSR30": {
        "title": "Benthic temperature record from Salt River, St. Croix, USVI",
        "location": "Salt River",
        "depth": "30 m",
        "site_description": "Down wall from to Salt River West in deep transects",
        "coordinates": "17.78523,-64.75917",
        "geospatial_lat_max": "17.78523",
        "geospatial_lat_min": "17.78523",
        "geospatial_lon_max": "-64.75917",
        "geospatial_lon_min": "-64.75917"
    },
    "TCSR99": {
        "title": "Benthic temperature record from Salt River, St. Croix, USVI",
        "location": "Salt River",
        "depth": "100 m",
        "site_description": "New site description for TCSR99",
        "coordinates": "17.78523",
        "geospatial_lat_max": "17.78523",
        "geospatial_lat_min": "17.78523",
        "geospatial_lon_max": "-64.75917",
        "geospatial_lon_min": "-64.75917"
    },
    "TCSR41": {
        "title": "Benthic temperature record from Salt River, St. Croix, USVI",
        "location": "Salt River",
        "depth": "41 m",
        "site_description": "Temperature probe only site",
        "coordinates": "17.78523,-64.75917",
        "geospatial_lat_max": "17.78523",
        "geospatial_lat_min": "17.78523",
        "geospatial_lon_max": "-64.75917",
        "geospatial_lon_min": "-64.75917"
    },
    "TCSR67": {
        "title": "Benthic temperature record from Salt River, St. Croix, USVI",
        "location": "Salt River",
        "depth": "67 m",
        "site_description": "New site description for TCSR67",
        "coordinates": "17.78523",
        "geospatial_lat_max": "17.78523",
        "geospatial_lat_min": "17.78523",
        "geospatial_lon_max": "-64.75917",
        "geospatial_lon_min": "-64.75917"
    },
    "TCSR10": {
        "title": "Benthic temperature record from Salt River, St. Croix, USVI",
        "location": "Salt River",
        "depth": "11 m",
        "site_description": "New site description for TCSR10",
        "coordinates": "17.7853, -64.7594",
        "geospatial_lat_max": "17.7853",
        "geospatial_lat_min": "17.7853",
        "geospatial_lon_max": "-64.7594",
        "geospatial_lon_min": "-64.7594"
    },
    "TCSPTH": {
        "title": "Benthic temperature record from Sprat Hole, St. Croix, USVI",
        "location": "Sprat Hole",
        "depth": "8 m",
        "site_description": "High density Orbicellia annularis reef near a dive mooring on west St. Croix north of Fredriksted",
        "coordinates": "17.734,-64.8954",
        "geospatial_lat_max": "17.734",
        "geospatial_lat_min": "17.734",
        "geospatial_lon_max": "-64.8954",
        "geospatial_lon_min": "-64.8954"
    }
    
}

# Variable attributes (same for all sites)
variable_attributes = {
    "Number": {
        "long_name": "Sequential number of the datum",
        "units": "1"
    },
    "Time": {
        "standard_name": "time",
        "long_name": "Time of measurement",
        "units": "seconds since 1970-01-01T00:00:00Z",
        "calendar": "gregorian",
        "comment": "Time in seconds since the Unix epoch"
    },
    "Temperature": {
        "standard_name": "sea_water_temperature",
        "long_name": "Water Temperature",
        "units": "degree_Celsius",
        "comment": "Water temperature recorded at site depth"
    },
    "Depth": {
        "standard_name": "depth",
        "long_name": "Depth below sea surface",
        "units": "m",
        "positive": "down",
        "axis": "Z"
    },
    "latitude": {
        "standard_name": "latitude",
        "long_name": "Latitude",
        "units": "degrees_north",
        "comment": "site latitude"
    },
    "longitude": {
        "standard_name": "longitude",
        "long_name": "Longitude",
        "units": "degrees_east",
        "comment": "site longitude"
    }
}

sub_codes = {
    "TCCORB": "CRB",
    "TCFSHB": "FSB",
    "TCMERI": "MSR",
    "TCBKPT": "BPT",
    "TCBOTB": "BTY",
    "TCBRWB": "BWR",
    "TCBKIT": "BIT",
    "TCCORK": "CKR",
    "TCCLGE": "CSE",
    "TCFLTC": "FLC",
    "TCGB63": "GBF",
    "TCGMKT": "GKT",
    "TCHB40": "HBE",
    "TCHB30": "HBE30",
    "TCHB20": "HBE20",
    "TCMAGB": "MGN",
    "TCSAVA": "SVN",
    "TCSHCS": "SHR",
    "TCSCAP": "SCP",
    "TCSC35": "SCPD",
    "TCSWAT": "SWT",
    "TCLSTJ": "SSJ",
    "TCBKIX": "BIX",
    "TCBX33": "BID",
    "TCCB08": "CBS",
    "TCCB40": "CBD",
    "TCCB99": "CBD99",
    "TCCB67": "CBD67",
    "TCCSTL": "CST",
    "TCEAGR": "EGR",
    "TCGRPD": "GRP",
    "TCJCKB": "JKB",
    "TCKNGC": "KGC",
    "TCLBEM": "LBP",
    "TCLB99": "LBP99",
    "TCLB67": "LBP67",
    "TCLBRH": "LBH",
    "TCMT24": "MTS",
    "TCMT40": "MTS40",
    "TCSR30": "SRD",
    "TCSR99": "SRD99",
    "TCSR41": "SRD41",
    "TCSR67": "SRD67",
    "TCSR10": "SRW",
    "TCSPTH": "SPH"
}


# Create YAML files
for site_code, info in site_modifications.items():
    metadata_yaml = {
        site_code: {
            "global_attributes": {
                "title": info.get("title", f"Benthic temperature record from {site_code}"),
                "institution": "University of the Virgin Islands",
                "location": info.get("location", site_code),
                "depth": info.get("depth", "Unknown"),
                "source": f"{site_code} Sensor Network",
                "coordinates": info.get("coordinates", "Unknown"),
                "geospatial_lat_max": info.get("geospatial_lat_max", "Unknown"),
                "geospatial_lat_min": info.get("geospatial_lat_min", "Unknown"),
                "geospatial_lon_max": info.get("geospatial_lon_max", "Unknown"),
                "geospatial_lon_min": info.get("geospatial_lon_min", "Unknown"),
                "site_description": info.get("site_description", ""),
                "sub_code": sub_codes.get(site_code, site_code),
                "cdm_data_type": "TimeSeries",
                "Conventions": "CF-1.6",
                "summary": f"Time series of benthic temperature recorded at {info.get('location', site_code)}, USVI",
                "project": "The United States Virgin Islands Territorial Coral Reef Monitoring Program",
                "funding": "Department of Planning and Natural Resources, NOAA Coral Reef Conservation Program",
                "contact": "Tyler B. Smith",
                "contact_email": "tsmith@uvi.edu",
                "device_name": "HOBO U22-001 Water Temperature",
                "creator_name": "Tyler B. Smith",
                "creator_email": "tsmith@uvi.edu",
                "creator_type": "person",
                "featureType": "timeSeries",
                "processing_level": "Data provided as is with no expressed or implied assurance of quality assurance or quality control.",
                "naming_authority": "https://www.uvi.edu/",
                "license": "This data may be redistributed and used without restriction. Data provided as is with no expressed or implied assurance of quality assurance or quality control",
                "id": site_code
            },
            "variable_attributes": variable_attributes
        }
    }

    file_path = os.path.join(output_folder, f"{site_code}.yaml")
    with open(file_path, "w") as f:
        yaml.dump(metadata_yaml, f, sort_keys=False)

print("YAML files created successfully!")

# %%
