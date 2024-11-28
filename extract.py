import json

class GeoJSONExtractor:
    def find_geometry(self, data):
        """
        Recursively search for GeoJSON geometry in the data.
        :param data: Dictionary that might contain GeoJSON geometry
        :return: The GeoJSON geometry (or None if not found)
        """
        if isinstance(data, dict):
            for key, value in data.items():
                if isinstance(value, dict):
                    if 'type' in value and 'coordinates' in value:
                        return value
                    geometry = self.find_geometry(value)
                    if geometry:
                        return geometry
        elif isinstance(data, list):
            for item in data:
                geometry = self.find_geometry(item)
                if geometry:
                    return geometry
        return None

    def flatten_properties(self, d):
        """
        Flatten a nested dictionary to extract leaf nodes only.
        :param d: Dictionary to flatten
        :return: Flattened dictionary
        """
        leaves = {}
        for key, value in d.items():
            if isinstance(value, dict):
                leaves.update(self.flatten_properties(value))
            elif not isinstance(value, list):  # Skip lists
                leaves[key] = value
        return leaves

    def convert_to_geojson(self, data_array, output_file):
        """
        Convert a list of data dictionaries into a GeoJSON FeatureCollection,
        handling cases with and without nesting.
        
        :param data_array: List of dictionaries containing 'geometry' and 'properties'
        :param output_file: The output file to save the GeoJSON data
        :return: GeoJSON FeatureCollection
        """
        features = []

        for data in data_array:
            # Flatten all parent-level properties
            parent_properties = self.flatten_properties(data)
            found_geometry = self.find_geometry(data)

            # If geometry is found at the root level, create a feature
            if found_geometry:
                geojson_feature = {
                    "type": "Feature",
                    "geometry": found_geometry,
                    "properties": parent_properties
                }
                features.append(geojson_feature)
                continue

            # If no root-level geometry, look for nested data structures
            for key, value in data.items():
                if isinstance(value, list):
                    for item in value:
                        # Flatten each nested structure and find geometry
                        nested_geometry = self.find_geometry(item)
                        nested_properties = self.flatten_properties(item)

                        # Combine parent properties with nested properties
                        combined_properties = {**parent_properties, **nested_properties}

                        if nested_geometry:
                            geojson_feature = {
                                "type": "Feature",
                                "geometry": nested_geometry,
                                "properties": combined_properties
                            }
                            features.append(geojson_feature)

        # Create a GeoJSON FeatureCollection
        geojson_collection = {
            "type": "FeatureCollection",
            "features": features
        }

        # Save GeoJSON data to the specified file
        with open(output_file, 'w') as f:
            json.dump(geojson_collection, f, indent=2)

        print(f"GeoJSON data saved to {output_file}")

        return geojson_collection


# Function to read data from a JSON file
def read_data_from_json(input_file):
    with open(input_file, 'r') as f:
        return json.load(f)

# Example usage:
input_file = 'submissions.json'  # Replace with your actual JSON file path
data_array = read_data_from_json(input_file)

# Create an instance of the GeoJSONExtractor
extractor = GeoJSONExtractor()

# Call the function to convert the data array to GeoJSON and save to a file
output_file = 'output.geojson'
geojson_result = extractor.convert_to_geojson(data_array, output_file)

# Print the resulting GeoJSON (optional)
print(json.dumps(geojson_result, indent=2))
