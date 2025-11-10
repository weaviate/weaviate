package filters

import (
    "encoding/json"
    "testing"

    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/require"
)

// These tests exercise ExtractPaginationFromArgs with values that can arrive
// from GraphQL variables (int64/json.Number/float64) to ensure no panics and
// correct coercion via graphqlutil.ToInt.
func TestExtractPaginationFromArgs_VariousTypes(t *testing.T) {
    // case: int64 values (common when JSON numbers are decoded by stdlib)
    args1 := map[string]interface{}{"offset": int64(2), "limit": int64(5), "autocut": int64(0)}
    p, err := ExtractPaginationFromArgs(args1)
    if err != nil {
        t.Fatalf("unexpected error: %v", err)
    }
    if p == nil {
        t.Fatalf("expected pagination, got nil")
    }
    if p.Offset != 2 || p.Limit != 5 || p.Autocut != 0 {
        t.Fatalf("unexpected pagination values: %+v", p)
    }

    // case: json.Number inputs (also a possible representation)
    args2 := map[string]interface{}{"offset": json.Number("3"), "limit": json.Number("10")}
    p2, err := ExtractPaginationFromArgs(args2)
    if err != nil {
        t.Fatalf("unexpected error: %v", err)
    }
    if p2 == nil {
        t.Fatalf("expected pagination, got nil")
    }
    if p2.Offset != 3 || p2.Limit != 10 {
        t.Fatalf("unexpected pagination values: %+v", p2)
    }

    // case: float64 non-integral should surface an error
    args3 := map[string]interface{}{"limit": 5.5}
    _, err = ExtractPaginationFromArgs(args3)
    if err == nil {
        t.Fatalf("expected error for non-integral float, got nil")
    }

    // case: no pagination args -> nil
    args4 := map[string]interface{}{"someOther": "x"}
    p4, err := ExtractPaginationFromArgs(args4)
    if err != nil {
        t.Fatalf("unexpected error: %v", err)
    }
    if p4 != nil {
        t.Fatalf("expected nil pagination when no args present, got %+v", p4)
    }
}

func TestExtractPagination(t *testing.T) {
    t.Run("without a limit present", func(t *testing.T) {
        p, err := ExtractPaginationFromArgs(map[string]interface{}{})
        require.Nil(t, err)
        assert.Nil(t, p)
    })

    t.Run("with a limit present", func(t *testing.T) {
        p, err := ExtractPaginationFromArgs(map[string]interface{}{
            "limit": 25,
        })
        require.Nil(t, err)
        require.NotNil(t, p)
        assert.Equal(t, 0, p.Offset)
        assert.Equal(t, 25, p.Limit)
    })

    t.Run("with a offset present", func(t *testing.T) {
        p, err := ExtractPaginationFromArgs(map[string]interface{}{
            "offset": 11,
        })
        require.Nil(t, err)
        require.NotNil(t, p)
        assert.Equal(t, 11, p.Offset)
        assert.Equal(t, -1, p.Limit)
    })

    t.Run("with offset and limit present", func(t *testing.T) {
        p, err := ExtractPaginationFromArgs(map[string]interface{}{
            "offset": 11,
            "limit":  25,
        })
        require.Nil(t, err)
        require.NotNil(t, p)
        assert.Equal(t, 11, p.Offset)
        assert.Equal(t, 25, p.Limit)
    })
}
