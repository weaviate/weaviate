package test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"google.golang.org/grpc"
)

const (
	envS3AccessKey           = "AWS_ACCESS_KEY_ID"
	envS3SecretKey           = "AWS_SECRET_KEY"
	s3BackupJourneyAccessKey = "aws_access_key"
	s3BackupJourneySecretKey = "aws_secret_key"
)

func newClient(t *testing.T, grpcHost string) (pb.WeaviateClient, *grpc.ClientConn) {
	conn, err := helper.CreateGrpcConnectionClient(grpcHost)
	require.NoError(t, err)
	require.NotNil(t, conn)
	grpcClient := helper.CreateGrpcWeaviateClient(conn)
	require.NotNil(t, grpcClient)
	return grpcClient, conn
}

func Test_Querier(t *testing.T) {
	t.Run("querier", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		t.Log("pre-instance env setup")
		t.Setenv(envS3AccessKey, s3BackupJourneyAccessKey)
		t.Setenv(envS3SecretKey, s3BackupJourneySecretKey)

		// TODO how to set the bucket in querier if not weaviate-offload?
		compose, err := docker.New().
			WithOffloadS3("weaviate-offload", "us-west-1").
			WithText2VecContextionary().
			WithWeaviateClusterWithGRPC().
			WithQuerier().
			Start(ctx)
		require.Nil(t, err)

		defer func() {
			if err := compose.Terminate(ctx); err != nil {
				t.Fatalf("failed to terminate test containers: %s", err.Error())
			}
		}()

		helper.SetupClient(compose.GetWeaviate().URI())
		coreGrpcURI := compose.GetWeaviate().GrpcURI()
		querierGrpcURI := compose.GetQuerier().GrpcURI()

		className := "MultiTenantClass"
		testClass := models.Class{
			Class:             className,
			ReplicationConfig: &models.ReplicationConfig{Factor: 2},
			MultiTenancyConfig: &models.MultiTenancyConfig{
				Enabled: true,
			},
			Properties: []*models.Property{
				{
					Name:     "name",
					DataType: schema.DataTypeText.PropString(),
				},
			},
		}
		t.Run("create class with multi-tenancy enabled", func(t *testing.T) {
			helper.CreateClass(t, &testClass)
		})

		tenantName := "Tenant1"
		t.Run("create tenant", func(t *testing.T) {
			helper.CreateTenants(t, className, []*models.Tenant{&models.Tenant{Name: tenantName}})
		})

		object := &models.Object{
			ID:    "0927a1e0-398e-4e76-91fb-04a7a8f0405c",
			Class: className,
			Properties: map[string]interface{}{
				"name": tenantName,
			},
			Tenant: tenantName,
		}

		defer func() {
			helper.DeleteClass(t, className)
		}()

		t.Run("add tenant objects", func(t *testing.T) {
			assert.Nil(t, helper.CreateObject(t, object))
		})

		t.Run("verify object creation", func(t *testing.T) {
			resp, err := helper.TenantObject(t, object.Class, object.ID, tenantName)
			require.Nil(t, err)
			assert.Equal(t, object.Class, resp.Class)
			assert.Equal(t, object.Properties, resp.Properties)
		})
		searchRequest := &pb.SearchRequest{
			Collection: className,
			Tenant:     tenantName,
			Metadata: &pb.MetadataRequest{
				Uuid: true,
			},
			Uses_127Api: true,
		}
		var coreResultObject *pb.SearchResult
		t.Run("verify object searchable on core", func(t *testing.T) {
			grpcClient, _ := newClient(t, coreGrpcURI)
			resp, err := grpcClient.Search(context.TODO(), searchRequest)
			require.NoError(t, err)
			require.Len(t, resp.Results, 1)
			coreResultObject = resp.Results[0]
			require.Equal(t, string(object.ID), coreResultObject.Metadata.Id)
			// object property name is set to the tenant name
			require.Equal(t, tenantName, coreResultObject.Properties.NonRefProps.Fields["name"].GetTextValue())
		})

		t.Run("verify tenant data version is 0", func(t *testing.T) {
			resp, err := helper.GetOneTenant(t, className, tenantName)
			require.Nil(t, err)
			require.Equal(t, tenantName, resp.Payload.Name)
			require.Equal(t, int64(0), *resp.Payload.DataVersion)
		})

		t.Run("updating tenant status", func(t *testing.T) {
			tenant := &models.Tenant{
				Name:           tenantName,
				ActivityStatus: models.TenantActivityStatusFROZEN,
			}
			helper.UpdateTenants(t, className, []*models.Tenant{tenant})
		})

		t.Run("verify tenant status FREEZING", func(t *testing.T) {
			resp, err := helper.GetTenants(t, className)
			require.Nil(t, err)
			require.Len(t, resp.Payload, 1)
			tn := resp.Payload[0]
			require.Equal(t, tenantName, tn.Name)
			require.Equal(t, models.TenantActivityStatusFREEZING, tn.ActivityStatus)
		})

		t.Run("verify tenant does not exists", func(t *testing.T) {
			_, err = helper.TenantObject(t, object.Class, object.ID, tenantName)
			require.NotNil(t, err)
		})

		t.Run("verify tenant status", func(t *testing.T) {
			assert.EventuallyWithT(t, func(at *assert.CollectT) {
				resp, err := helper.GetTenants(t, className)
				require.Nil(at, err)
				require.Len(at, resp.Payload, 1)
				require.Equal(at, models.TenantActivityStatusFROZEN, resp.Payload[0].ActivityStatus)
			}, 5*time.Second, time.Second, fmt.Sprintf("tenant was never %s", models.TenantActivityStatusFROZEN))
		})

		t.Run("verify tenant data version is 1 after freezing", func(t *testing.T) {
			resp, err := helper.GetOneTenant(t, className, tenantName)
			require.Nil(t, err)
			require.Equal(t, tenantName, resp.Payload.Name)
			require.Equal(t, int64(1), *resp.Payload.DataVersion)
		})

		t.Run("verify objects searchable on querier", func(t *testing.T) {
			// TODO minio error unclear from response/logs
			// NATEE querier resp <nil> rpc error: code = Unknown desc = failed to fetch tenant data: NotFound: Not Found
			// status code: 404, request id: 180982A352A7D338, host id: dd9025bab4ad464b049177c95eb6ebf374d3b3fd1af9251148b658df7ac2e3e8, "MultiTenantClass", "Tenant1"
			grpcClient, _ := newClient(t, querierGrpcURI)
			resp, err := grpcClient.Search(context.TODO(), searchRequest)
			require.NoError(t, err)
			require.Len(t, resp.Results, 1)
			querierResultObject := resp.Results[0]
			require.Equal(t, coreResultObject.Metadata.Id, querierResultObject.Metadata.Id)
			// object property name is set to the tenant name
			require.Equal(t, coreResultObject.Properties.NonRefProps.Fields["name"].GetTextValue(), querierResultObject.Properties.NonRefProps.Fields["name"].GetStringValue())
		})
	})
}
