
from utils import get_minio_client
import json
import pandas as pd
import io
import datetime


def list_all_metadata():
    """List all metadata stored in the govern-zone-metadata bucket."""
    client = get_minio_client()

    if not client.bucket_exists('govern-zone-metadata'):
        print("Govern zone metadata bucket does not exist.")
        return {}

    print("Retrieving metadata catalog from govern-zone-metadata:")

    # Collect all metadata objects
    objects = list(client.list_objects('govern-zone-metadata', prefix='metadata/'))

    metadata_catalog = {}
    for obj in objects:
        try:
            # Download metadata
            response = client.get_object('govern-zone-metadata', obj.object_name)
            metadata_json = json.loads(response.read().decode('utf-8'))

            # Extract source information
            source_bucket = metadata_json.get('source_bucket', 'unknown')
            object_name = metadata_json.get('object_name', 'unknown')

            # Build catalog structure
            if source_bucket not in metadata_catalog:
                metadata_catalog[source_bucket] = {}

            metadata_catalog[source_bucket][object_name] = metadata_json
        except Exception as e:
            print(f"Error reading metadata for {obj.object_name}: {e}")

    return metadata_catalog

def trace_data_lineage(target_object, target_bucket='access-zone'):
    """Trace the lineage of a specific dataset back to its origins."""
    client = get_minio_client()

    if not client.bucket_exists('govern-zone-metadata'):
        print("Govern zone metadata bucket does not exist.")
        return []

    print(f"Tracing data lineage for {target_bucket}/{target_object}:")

    # Find lineage objects where this dataset is the target
    lineage_objects = list(client.list_objects('govern-zone-metadata', prefix='lineage/'))

    # Build the lineage chain
    lineage_chain = []
    current_target = (target_bucket, target_object)

    while current_target:
        found_source = False

        for obj in lineage_objects:
            try:
                response = client.get_object('govern-zone-metadata', obj.object_name)
                lineage = json.loads(response.read().decode('utf-8'))

                # Check if this lineage record has our current target
                if (lineage['target']['bucket'] == current_target[0] and
                        lineage['target']['object'] == current_target[1]):

                    # Add to lineage chain
                    lineage_chain.append(lineage)

                    # Move to the source of this transformation
                    if lineage['source'] != 'multiple':
                        current_target = (lineage['source']['bucket'], lineage['source']['object'])
                        found_source = True
                        break
                    else:
                        # Multiple sources - need to handle differently
                        lineage_chain.append({
                            'timestamp': lineage['timestamp'],
                            'note': 'This dataset was created from multiple source datasets',
                            'transformation': lineage['transformation']
                        })
                        current_target = None
                        break
            except Exception as e:
                print(f"Error reading lineage for {obj.object_name}: {e}")

        # If we didn't find a source, we've reached the beginning of the chain
        if not found_source:
            current_target = None

    # Reverse to get chronological order
    lineage_chain.reverse()
    return lineage_chain

def generate_data_quality_report():
    """Generate a report of data quality checks."""
    client = get_minio_client()

    if not client.bucket_exists('govern-zone-metadata'):
        print("Govern zone metadata bucket does not exist.")
        return pd.DataFrame()

    print("Generating data quality report:")

    # Find quality check objects
    quality_objects = list(client.list_objects('govern-zone-metadata', prefix='quality/'))

    # Collect quality check results
    quality_results = []
    for obj in quality_objects:
        try:
            response = client.get_object('govern-zone-metadata', obj.object_name)
            quality_check = json.loads(response.read().decode('utf-8'))

            # Process each individual check
            for check in quality_check['checks']:
                result = {
                    'dataset': quality_check['dataset'],
                    'timestamp': quality_check['timestamp'],
                    'check_type': check['check'],
                    'column': check['column'],
                    'passed': check['passed'],
                    'details': check['details']
                }
                quality_results.append(result)
        except Exception as e:
            print(f"Error reading quality check for {obj.object_name}: {e}")

    # Convert to DataFrame
    return pd.DataFrame(quality_results)



def main():
    print("Demonstrating Govern Zone functionality...\n")

    # 2. Trace data lineage for an analytics dataset
    print("\n\n=== Data Lineage Tracing ===")
    lineage = trace_data_lineage('analytics/rutas_users.parquet')

    # Print lineage
    if lineage:
        print("\nLineage chain:")
        for step_num, step in enumerate(lineage, 1):
            print(f"\nStep {step_num}:")
            if 'note' in step:
                print(f"  Note: {step['note']}")
            else:
                print(f"  From: {step['source']['bucket']}/{step['source']['object']}")
                print(f"  To: {step['target']['bucket']}/{step['target']['object']}")
            print(f"  Transformation: {step['transformation']}")
            print(f"  Timestamp: {step['timestamp']}")
    else:
        print("No lineage information found.")

    # 3. Generate data quality report
    print("\n\n=== Data Quality Report ===")
    quality_report = generate_data_quality_report()

    if not quality_report.empty:
        # Print quality report summary
        print("\nQuality Check Summary:")
        quality_summary = quality_report.groupby(['dataset', 'check_type']).agg({
            'passed': ['sum', 'count'],
        }).reset_index()
        quality_summary.columns = ['dataset', 'check_type', 'passed_count', 'total_count']
        quality_summary['pass_rate'] = quality_summary['passed_count'] / quality_summary['total_count'] * 100

        for _, row in quality_summary.iterrows():
            print(f"\n  Dataset: {row['dataset']}")
            print(f"  Check Type: {row['check_type']}")
            print(f"  Pass Rate: {row['pass_rate']:.1f}% ({row['passed_count']}/{row['total_count']})")

        # List failed checks
        failed_checks = quality_report[~quality_report['passed']].copy()
        if not failed_checks.empty:
            print("\nFailed Quality Checks:")
            for _, check in failed_checks.iterrows():
                print(f"  - {check['dataset']}: {check['check_type']} on column '{check['column']}' failed")
                print(f"    Details: {check['details']}")
    else:
        print("No quality check results found.")



    # Summary
    print("\n\n=== Govern Zone Summary ===")
    print("The Govern Zone provides:")
    print("1. Comprehensive metadata management")
    print("2. Data lineage tracking for all datasets")
    print("3. Data quality monitoring and reporting")
    print("4. Security and access control policies")
    print("5. Audit logs and compliance monitoring")
    print("\nThese governance capabilities ensure data is trustworthy, secure, and compliant with regulations.")

if __name__ == "__main__":
    main()