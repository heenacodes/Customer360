import boto3
rds = boto3.client("rds", region_name="us-east-1")

def create_rds_instance(db_instance_id, master_username, master_password):
    """
    Create an Amazon RDS MySQL instance.

    :param db_instance_id: RDS instance identifier
    :param master_username: Master username
    :param master_password: Master password
    """

    try:
        response = rds.create_db_instance(
            DBInstanceIdentifier=db_instance_id,
            AllocatedStorage=20,  # 20GB storage
            DBInstanceClass="db.t3.micro",  # Free-tier instance type
            Engine="mysql",
            MasterUsername=master_username,
            MasterUserPassword=master_password,
            DBName="mydb",
            PubliclyAccessible=True,  # Change to False if private
            BackupRetentionPeriod=7,  # Days to retain backups
            MultiAZ=False,  # Single AZ deployment
            StorageType="gp2"
        )

        print(f"RDS MySQL instance '{db_instance_id}' is being created.")
    except Exception as e:
        print(f"Error creating RDS instance: {e}")

def get_rds_endpoint(db_instance_id):
    """
    Get the endpoint (host) of an RDS instance.

    :param db_instance_id: RDS instance identifier
    """

    try:
        response = rds.describe_db_instances(DBInstanceIdentifier=db_instance_id)
        endpoint = response["DBInstances"][0]["Endpoint"]["Address"]
        print(f"RDS Endpoint: {endpoint}")
        return endpoint
    except Exception as e:
        print(f"Error retrieving endpoint: {e}")


def delete_rds_instance(db_instance_id):
    """
    Delete an RDS MySQL instance.

    :param db_instance_id: RDS instance identifier
    """

    try:
        response = rds.delete_db_instance(
            DBInstanceIdentifier=db_instance_id,
            SkipFinalSnapshot=True  # Change to False if you want a final snapshot
        )
        print(f"RDS MySQL instance '{db_instance_id}' is being deleted.")
    except Exception as e:
        print(f"Error deleting RDS instance: {e}")


#get_rds_endpoint("customer360-mysql-dev")


#delete_rds_instance("customer360-mysql-dev")

create_rds_instance(db_instance_id="customer360-mysql-dev", master_username="admin",  master_password="YourPassword")

