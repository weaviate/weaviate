package test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/cluster/types"
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

// TODO dry
func newClient(t *testing.T, grpcHost string) (pb.WeaviateClient, *grpc.ClientConn) {
	// conn, err := helper.CreateGrpcConnectionClient(":50051")
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
			// With3NodeCluster().
			WithWeaviateClusterWithGRPC().
			WithQuerier().
			Start(ctx)
		require.Nil(t, err)

		// fmt.Println("NATEE start sleep")
		// time.Sleep(9999 * time.Second)

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

		tenantNames := []string{
			"Tenant1", "Tenant2", "Tenant3",
		}
		t.Run("create tenants", func(t *testing.T) {
			tenants := make([]*models.Tenant, len(tenantNames))
			for i := range tenants {
				tenants[i] = &models.Tenant{Name: tenantNames[i]}
			}
			helper.CreateTenants(t, className, tenants)
		})

		tenantObjects := []*models.Object{
			{
				ID:    "0927a1e0-398e-4e76-91fb-04a7a8f0405c",
				Class: className,
				Properties: map[string]interface{}{
					"name": tenantNames[0],
				},
				Tenant: tenantNames[0],
			},
			{
				ID:    "831ae1d0-f441-44b1-bb2a-46548048e26f",
				Class: className,
				Properties: map[string]interface{}{
					"name": tenantNames[1],
				},
				Tenant: tenantNames[1],
			},
			{
				ID:    "6f3363e0-c0a0-4618-bf1f-b6cad9cdff59",
				Class: className,
				Properties: map[string]interface{}{
					"name": tenantNames[2],
				},
				Tenant: tenantNames[2],
			},
		}

		defer func() {
			helper.DeleteClass(t, className)
		}()

		t.Run("add tenant objects", func(t *testing.T) {
			for _, obj := range tenantObjects {
				assert.Nil(t, helper.CreateObject(t, obj))
			}
		})

		t.Run("verify object creation", func(t *testing.T) {
			for i, obj := range tenantObjects {
				resp, err := helper.TenantObject(t, obj.Class, obj.ID, tenantNames[i])
				require.Nil(t, err)
				assert.Equal(t, obj.Class, resp.Class)
				assert.Equal(t, obj.Properties, resp.Properties)
			}
		})

		t.Run("verify objects searchable on core", func(t *testing.T) {
			grpcClient, _ := newClient(t, coreGrpcURI)
			resp, err := grpcClient.Search(context.TODO(), &pb.SearchRequest{
				Collection: className,
				Tenant:     tenantNames[0],
				Metadata: &pb.MetadataRequest{
					Uuid: true,
				},
				// Uses_123Api: true,
				// Uses_125Api: true,
				Uses_127Api: true,
			})
			fmt.Println("NATEE resp", resp, err)
			for _, r := range resp.Results {
				fmt.Println("NATEE ID", r.Metadata.Id)
				fmt.Println("NATEE Name", r.Properties.NonRefProps.Fields["name"].GetTextValue())
			}
		})

		t.Run("verify tenant data version is 0", func(t *testing.T) {
			for _, tn := range tenantNames {
				resp, err := helper.GetOneTenant(t, className, tn)
				require.Nil(t, err)
				require.Equal(t, tn, resp.Payload.Name)
				require.Equal(t, int64(0), *resp.Payload.DataVersion)
			}
		})

		t.Run("updating tenant status", func(t *testing.T) {
			tenants := []*models.Tenant{}
			for i := range tenantNames {
				tenants = append(tenants, &models.Tenant{
					Name:           tenantNames[i],
					ActivityStatus: models.TenantActivityStatusFROZEN,
				})
			}

			helper.UpdateTenants(t, className, tenants)
		})

		t.Run("verify tenant status FREEZING", func(t *testing.T) {
			resp, err := helper.GetTenants(t, className)
			require.Nil(t, err)
			for _, tn := range resp.Payload {
				for i := range tenantNames {
					if tn.Name == tenantNames[i] {
						require.Equal(t, types.TenantActivityStatusFREEZING, tn.ActivityStatus)
						break
					}
				}
			}
		})

		t.Run("verify tenant does not exists", func(t *testing.T) {
			for i := range tenantObjects {
				_, err = helper.TenantObject(t, tenantObjects[i].Class, tenantObjects[i].ID, tenantNames[i])
				require.NotNil(t, err)
			}
		})

		t.Run("verify tenant status", func(t *testing.T) {
			assert.EventuallyWithT(t, func(at *assert.CollectT) {
				resp, err := helper.GetTenants(t, className)
				require.Nil(t, err)
				for _, tn := range resp.Payload {
					for i := range tenantNames {
						if tn.Name == tenantNames[i] {
							assert.Equal(at, models.TenantActivityStatusFROZEN, tn.ActivityStatus)
							break
						}
					}
				}
			}, 5*time.Second, time.Second, fmt.Sprintf("tenant was never %s", models.TenantActivityStatusFROZEN))
		})

		t.Run("verify tenant data version is 1 after freezing", func(t *testing.T) {
			for _, tn := range tenantNames {
				resp, err := helper.GetOneTenant(t, className, tn)
				require.Nil(t, err)
				require.Equal(t, tn, resp.Payload.Name)
				require.Equal(t, int64(1), *resp.Payload.DataVersion)
			}
		})

		t.Run("verify objects searchable on querier", func(t *testing.T) {
			// TODO minio error unclear from response/logs
			// NATEE querier resp <nil> rpc error: code = Unknown desc = failed to fetch tenant data: NotFound: Not Found
			// status code: 404, request id: 180982A352A7D338, host id: dd9025bab4ad464b049177c95eb6ebf374d3b3fd1af9251148b658df7ac2e3e8, "MultiTenantClass", "Tenant1"
			grpcClient, _ := newClient(t, querierGrpcURI)
			resp, err := grpcClient.Search(context.TODO(), &pb.SearchRequest{
				Collection: className,
				Tenant:     tenantNames[0],
				Metadata: &pb.MetadataRequest{
					Uuid: true,
				},
				// Uses_123Api: true,
				// Uses_125Api: true,
				Uses_127Api: true,
			})
			fmt.Println("NATEE querier resp", resp, err)
			// time.Sleep(9999 * time.Second)
			for _, r := range resp.Results {
				fmt.Println("NATEE querier ID", r.Metadata.Id)
				fmt.Println("NATEE querier Name", r.Properties.NonRefProps.Fields["name"].GetStringValue())
				fmt.Println("NATEE querier Name as text", r.Properties.NonRefProps.Fields["name"].GetTextValue())
			}
		})
	})
}
