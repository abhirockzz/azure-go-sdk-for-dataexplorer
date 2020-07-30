package ops

import (
	"context"
	"log"

	"github.com/Azure/azure-sdk-for-go/services/kusto/mgmt/2020-02-15/kusto"
)

// ListDBs gets all DBs in an ADX cluster
func ListDBs(sub, rgName, clusterName string) kusto.DatabaseListResult {
	ctx := context.Background()
	result, err := getDBClient(sub).ListByCluster(ctx, rgName, clusterName)
	if err != nil {
		log.Fatal("failed to get databases in cluster", err)
	}

	return result
}

// CreateDatabase creates a database in a give ADX cluster
func CreateDatabase(sub, rgName, clusterName, location, dbName string) {
	ctx := context.Background()

	client := getDBClient(sub)
	future, err := client.CreateOrUpdate(ctx, rgName, clusterName, dbName, kusto.ReadWriteDatabase{Kind: kusto.KindReadWrite, Location: &location})

	if err != nil {
		log.Fatal("failed to start database creation ", err)
	}

	log.Println("waiting for database creation to complete....")
	err = future.WaitForCompletionRef(context.Background(), client.Client)
	if err != nil {
		log.Fatal("failed to create database", err)
	}

	r, err := future.Result(client)
	if err != nil {
		log.Fatal("database creation process has not yet completed", err)
	}
	kdb, _ := r.Value.AsReadWriteDatabase()
	log.Printf("created DB %s with ID %s and type %s", *kdb.Name, *kdb.ID, *kdb.Type)
}

// DeleteDB removes database from an ADX cluster
func DeleteDB(sub, rgName, clusterName, dbName string) {
	ctx := context.Background()

	client := getDBClient(sub)
	future, err := getDBClient(sub).Delete(ctx, rgName, clusterName, dbName)

	if err != nil {
		log.Fatal("failed to start database deletion ", err)
	}

	log.Println("waiting for database deletion to complete....")
	err = future.WaitForCompletionRef(context.Background(), client.Client)
	if err != nil {
		log.Fatal("failed to delete database", err)
	}

	r, err := future.Result(client)
	if err != nil {
		log.Fatal("database deletion process has not yet completed", err)
	}

	if r.StatusCode == 200 {
		log.Printf("deleted DB %s from cluster %s", dbName, clusterName)
	} else {
		log.Println("failed to delete DB. response status code -", r.StatusCode)
	}
}
