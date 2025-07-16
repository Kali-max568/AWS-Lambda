import boto3
import logging

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Create EC2 client (auto picks region from Lambda config)
ec2 = boto3.client('ec2')

def lambda_handler(event, context):
    logger.info("Starting snapshot cleanup Lambda...")

    # Step 1: Fetch all snapshots owned by the current AWS account
    logger.info("Fetching EBS snapshots owned by this account...")
    snapshots = ec2.describe_snapshots(OwnerIds=['self'])['Snapshots']
    logger.info(f"Total snapshots found: {len(snapshots)}")

    # Step 2: Fetch all running and stopped EC2 instances
    logger.info("Fetching running and stopped EC2 instances...")
    instance_states = ['running', 'stopped']
    reservations = ec2.describe_instances(
        Filters=[{'Name': 'instance-state-name', 'Values': instance_states}]
    )['Reservations']

    # Step 3: Extract all volume IDs attached to active instances
    active_volume_ids = set()
    for reservation in reservations:
        for instance in reservation['Instances']:
            for mapping in instance.get('BlockDeviceMappings', []):
                volume_id = mapping['Ebs']['VolumeId']
                active_volume_ids.add(volume_id)

    logger.info(f"Active volumes attached to instances: {len(active_volume_ids)}")

    # Step 4: Loop through snapshots to find and delete stale ones
    stale_snapshots = 0
    for snapshot in snapshots:
        snap_id = snapshot['SnapshotId']
        volume_id = snapshot.get('VolumeId')

        if not volume_id:
            logger.warning(f"Snapshot {snap_id} has no VolumeId. Skipping...")
            continue

        if volume_id not in active_volume_ids:
            # Snapshot is stale — volume not attached to any active instance
            try:
                logger.info(f"Deleting stale snapshot {snap_id} (VolumeId: {volume_id})...")
                ec2.delete_snapshot(SnapshotId=snap_id)
                stale_snapshots += 1
            except Exception as e:
                logger.error(f"Error deleting snapshot {snap_id}: {e}")
        else:
            logger.info(f"Snapshot {snap_id} is associated with active volume {volume_id}. Keeping it.")

    logger.info(f"Cleanup complete. Total stale snapshots deleted: {stale_snapshots}")

