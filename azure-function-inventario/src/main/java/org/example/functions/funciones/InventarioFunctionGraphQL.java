package org.example.functions.funciones;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.functions.*;
import com.microsoft.azure.functions.annotation.*;
import graphql.ExecutionInput;
import graphql.ExecutionResult;
import graphql.GraphQL;
import graphql.GraphQLContext;
import graphql.Scalars;
import graphql.schema.*;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;
import java.util.logging.Logger;

public class InventarioFunctionGraphQL {
    private static GraphQL graphQL;

    private static Logger safeLogger(Object o) {
        if (o instanceof Logger l) return l;
        return Logger.getLogger("GraphQL");
    }
    private static long toId(Object raw) {
        if (raw instanceof Number n) return n.longValue();
        if (raw instanceof char[] ca) return Long.parseLong(new String(ca));
        return Long.parseLong(String.valueOf(raw));
    }
    private static Long toLong(Object o) {
        if (o == null) return null;
        try { return Long.valueOf(String.valueOf(o)); } catch (Exception e) { return null; }
    }
    private static Integer toInt(Object o) {
        if (o == null) return null;
        try { return Integer.valueOf(String.valueOf(o)); } catch (Exception e) { return null; }
    }

    static {
        GraphQLObjectType inventarioType = GraphQLObjectType.newObject()
                .name("Inventario")
                .field(f -> f.name("id").type(Scalars.GraphQLID))
                .field(f -> f.name("idProducto").type(new GraphQLNonNull(Scalars.GraphQLInt)))
                .field(f -> f.name("cantidadProductos").type(new GraphQLNonNull(Scalars.GraphQLInt)))
                .field(f -> f.name("idBodega").type(new GraphQLNonNull(Scalars.GraphQLInt)))
                .build();

        GraphQLInputObjectType inventarioInput = GraphQLInputObjectType.newInputObject()
                .name("InventarioInput")
                .field(f -> f.name("idProducto").type(new GraphQLNonNull(Scalars.GraphQLInt)))
                .field(f -> f.name("cantidadProductos").type(new GraphQLNonNull(Scalars.GraphQLInt)))
                .field(f -> f.name("idBodega").type(new GraphQLNonNull(Scalars.GraphQLInt)))
                .build();

        DataFetcher<Map<String, Object>> inventarioByIdFetcher = env -> {
            Logger log = safeLogger(env.getGraphQlContext().get("logger"));
            long id = toId(env.getArgument("id"));
            log.info("inventarioById id=" + id);
            return obtenerInventarioById(id, log);
        };

        DataFetcher<List<Map<String, Object>>> inventariosFetcher = env ->
                obtenerInventarios(safeLogger(env.getGraphQlContext().get("logger")));

        DataFetcher<Map<String, Object>> crearInventarioFetcher = env -> {
            Logger log = safeLogger(env.getGraphQlContext().get("logger"));
            Map<String, Object> input = env.getArgument("input");
            Long idProducto = toLong(input.get("idProducto"));
            Integer cantidad = toInt(input.get("cantidadProductos"));
            Long idBodega = toLong(input.get("idBodega"));
            if (idProducto == null || cantidad == null || idBodega == null) {
                throw new IllegalArgumentException("idProducto, cantidadProductos e idBodega son obligatorios");
            }
            long newId = insertInventario(idProducto, cantidad, idBodega, log);
            return obtenerInventarioById(newId, log);
        };

        DataFetcher<Map<String, Object>> actualizarInventarioFetcher = env -> {
            Logger log = safeLogger(env.getGraphQlContext().get("logger"));
            long id = toId(env.getArgument("id"));
            Map<String, Object> input = env.getArgument("input");
            Long idProducto = toLong(input.get("idProducto"));
            Integer cantidad = toInt(input.get("cantidadProductos"));
            Long idBodega = toLong(input.get("idBodega"));
            boolean ok = actualizarInventario(id, idProducto, cantidad, idBodega, log);
            if (!ok) return null; // no existe
            return obtenerInventarioById(id, log);
        };

        DataFetcher<Boolean> eliminarInventarioFetcher = env -> {
            Logger log = safeLogger(env.getGraphQlContext().get("logger"));
            long id = toId(env.getArgument("id"));
            return eliminarInventarioById(id, log);
        };

        GraphQLObjectType queryType = GraphQLObjectType.newObject()
                .name("Query")
                .field(f -> f.name("inventario")
                        .type(inventarioType)
                        .argument(GraphQLArgument.newArgument().name("id").type(new GraphQLNonNull(Scalars.GraphQLID)))
                        .dataFetcher(inventarioByIdFetcher))
                .field(f -> f.name("inventarios")
                        .type(GraphQLList.list(inventarioType))
                        .dataFetcher(inventariosFetcher))
                .build();

        GraphQLObjectType mutationType = GraphQLObjectType.newObject()
                .name("Mutation")
                .field(f -> f.name("crearInventario")
                        .type(inventarioType)
                        .argument(GraphQLArgument.newArgument().name("input").type(new GraphQLNonNull(inventarioInput)))
                        .dataFetcher(crearInventarioFetcher))
                .field(f -> f.name("actualizarInventario")
                        .type(inventarioType)
                        .argument(GraphQLArgument.newArgument().name("id").type(new GraphQLNonNull(Scalars.GraphQLID)))
                        .argument(GraphQLArgument.newArgument().name("input").type(new GraphQLNonNull(inventarioInput)))
                        .dataFetcher(actualizarInventarioFetcher))
                .field(f -> f.name("eliminarInventario")
                        .type(Scalars.GraphQLBoolean)
                        .argument(GraphQLArgument.newArgument().name("id").type(new GraphQLNonNull(Scalars.GraphQLID)))
                        .dataFetcher(eliminarInventarioFetcher))
                .build();

        graphQL = GraphQL.newGraphQL(
                GraphQLSchema.newSchema().query(queryType).mutation(mutationType).build()
        ).build();
    }

    @FunctionName("graphqlInventarios")
    public HttpResponseMessage handleGraphQL(
            @HttpTrigger(
                    name = "req",
                    methods = {HttpMethod.POST, HttpMethod.GET},
                    route = "graphql/inventarios",
                    authLevel = AuthorizationLevel.ANONYMOUS
            ) HttpRequestMessage<Optional<String>> request,
            final ExecutionContext context
    ) {
        Logger log = context.getLogger();
        String query = null;
        Map<String, Object> variables = new HashMap<>();

        try {
            if (request.getHttpMethod() == HttpMethod.POST) {
                String body = request.getBody().orElse("");
                if (!body.isBlank()) {
                    Map<String, Object> parsed = SimpleJson.parseJsonObject(body);
                    Object q = parsed.get("query");
                    if (q != null) query = String.valueOf(q);
                    Object vars = parsed.get("variables");
                    if (vars instanceof Map) variables = (Map<String, Object>) vars;
                    else if (vars instanceof String && !String.valueOf(vars).isBlank()) {
                        Object obj = SimpleJson.parse(String.valueOf(vars));
                        if (obj instanceof Map) variables = (Map<String, Object>) obj;
                    }
                }
            } else {
                query = request.getQueryParameters().get("query");
                String varsStr = request.getQueryParameters().get("variables");
                if (varsStr != null && !varsStr.isBlank()) {
                    Object obj = SimpleJson.parse(varsStr);
                    if (obj instanceof Map) variables = (Map<String, Object>) obj;
                }
            }
        } catch (Exception e) {
            log.severe("Error parseando request GraphQL: " + e.getMessage());
            return request.createResponseBuilder(HttpStatus.BAD_REQUEST)
                    .body(Map.of("errors", List.of(Map.of("message", "Invalid request payload"))))
                    .header("Content-Type", "application/json").build();
        }

        if (query == null || query.isBlank()) {
            return request.createResponseBuilder(HttpStatus.BAD_REQUEST)
                    .body(Map.of("errors", List.of(Map.of("message", "Missing 'query'"))))
                    .header("Content-Type", "application/json").build();
        }

        GraphQLContext gqlCtx = GraphQLContext.newContext().of("logger", log).build();
        ExecutionInput executionInput = ExecutionInput.newExecutionInput()
                .query(query).variables(variables).context(gqlCtx).build();

        ExecutionResult result = graphQL.execute(executionInput);
        return request.createResponseBuilder(HttpStatus.OK)
                .body(result.toSpecification())
                .header("Content-Type", "application/json").build();
    }


    private static Map<String, Object> obtenerInventarioById(long id, Logger log) throws Exception {
        if (log == null) log = Logger.getLogger("GraphQL");
        try (Connection conn = open();
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT ID, ID_PRODUCTO, CANTIDAD_PRODUCTOS, ID_BODEGA FROM INVENTARIO WHERE ID=?")) {
            ps.setLong(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return null;
                Map<String, Object> m = new HashMap<>();

                Object idObj = rs.getObject("ID");
                if (idObj instanceof java.math.BigDecimal bd) m.put("id", bd.longValue());
                else if (idObj instanceof Number n)           m.put("id", n.longValue());
                else                                          m.put("id", Long.parseLong(String.valueOf(idObj)));

                m.put("idProducto", ((Number)rs.getObject("ID_PRODUCTO")).longValue());
                m.put("cantidadProductos", ((Number)rs.getObject("CANTIDAD_PRODUCTOS")).intValue());
                m.put("idBodega", ((Number)rs.getObject("ID_BODEGA")).longValue());
                return m;
            }
        }
    }

    private static List<Map<String, Object>> obtenerInventarios(Logger log) throws Exception {
        if (log == null) log = Logger.getLogger("GraphQL");
        try (Connection conn = open();
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT ID, ID_PRODUCTO, CANTIDAD_PRODUCTOS, ID_BODEGA FROM INVENTARIO ORDER BY ID");
             ResultSet rs = ps.executeQuery()) {
            List<Map<String, Object>> list = new ArrayList<>();
            while (rs.next()) {
                Map<String, Object> m = new HashMap<>();

                Object idObj = rs.getObject("ID");
                if (idObj instanceof java.math.BigDecimal bd) m.put("id", bd.longValue());
                else if (idObj instanceof Number n)           m.put("id", n.longValue());
                else                                          m.put("id", Long.parseLong(String.valueOf(idObj)));

                m.put("idProducto", ((Number)rs.getObject("ID_PRODUCTO")).longValue());
                m.put("cantidadProductos", ((Number)rs.getObject("CANTIDAD_PRODUCTOS")).intValue());
                m.put("idBodega", ((Number)rs.getObject("ID_BODEGA")).longValue());
                list.add(m);
            }
            return list;
        }
    }

    private static long insertInventario(Long idProducto, Integer cantidad, Long idBodega, Logger log) throws Exception {
        if (log == null) log = Logger.getLogger("GraphQL");
        String sql = "INSERT INTO INVENTARIO (ID_PRODUCTO, CANTIDAD_PRODUCTOS, ID_BODEGA) VALUES (?, ?, ?)";
        try (Connection conn = open();
             PreparedStatement ps = conn.prepareStatement(sql, new String[] { "ID" })) {
            ps.setLong(1, idProducto);
            ps.setInt(2, cantidad);
            ps.setLong(3, idBodega);
            ps.executeUpdate();
            try (ResultSet keys = ps.getGeneratedKeys()) {
                if (keys != null && keys.next()) {
                    java.math.BigDecimal k = keys.getBigDecimal(1);
                    if (k != null) return k.longValue();
                }
            }
        }
        // Fallback si no hay generated keys
        try (Connection conn = open();
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT ID FROM INVENTARIO WHERE ID_PRODUCTO=? AND CANTIDAD_PRODUCTOS=? AND ID_BODEGA=? " +
                             "ORDER BY ID DESC FETCH FIRST 1 ROWS ONLY")) {
            ps.setLong(1, idProducto);
            ps.setInt(2, cantidad);
            ps.setLong(3, idBodega);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return rs.getLong(1);
            }
        }
        throw new SQLException("No fue posible obtener el ID generado");
    }

    private static boolean actualizarInventario(long id, Long idProducto, Integer cantidad, Long idBodega, Logger log) throws Exception {
        if (log == null) log = Logger.getLogger("GraphQL");
        // update parcial: solo setea lo no-nulo
        List<String> sets = new ArrayList<>();
        if (idProducto != null) sets.add("ID_PRODUCTO=?");
        if (cantidad != null) sets.add("CANTIDAD_PRODUCTOS=?");
        if (idBodega != null) sets.add("ID_BODEGA=?");
        if (sets.isEmpty()) return true;

        String sql = "UPDATE INVENTARIO SET " + String.join(", ", sets) + " WHERE ID=?";
        try (Connection conn = open(); PreparedStatement ps = conn.prepareStatement(sql)) {
            int i = 1;
            if (idProducto != null) ps.setLong(i++, idProducto);
            if (cantidad != null) ps.setInt(i++, cantidad);
            if (idBodega != null) ps.setLong(i++, idBodega);
            ps.setLong(i, id);
            int rows = ps.executeUpdate();
            return rows > 0;
        }
    }

    private static boolean eliminarInventarioById(long id, Logger log) throws Exception {
        if (log == null) log = Logger.getLogger("GraphQL");
        try (Connection conn = open();
             PreparedStatement ps = conn.prepareStatement("DELETE FROM INVENTARIO WHERE ID=?")) {
            ps.setLong(1, id);
            return ps.executeUpdate() > 0;
        }
    }

    private static Connection open() throws SQLException {
        String walletPath = System.getenv("ORACLE_WALLET_DIR");
        if (walletPath == null || walletPath.isBlank()) {
            walletPath = "/Users/franciscapalma/Desktop/Bimestre VI/Cloud Native II/Semana 3/azure-project/Wallet_DQXABCOJF1X64NFC";
        }
        String url = "jdbc:oracle:thin:@dqxabcojf1x64nfc_tp?TNS_ADMIN=" + walletPath;
        String user = "usuario_test";
        String pass = "Usuariotest2025";
        return DriverManager.getConnection(url, user, pass);
    }

    static class SimpleJson {
        private static final ObjectMapper MAPPER = new ObjectMapper();

        @SuppressWarnings("unchecked")
        static Map<String, Object> parseJsonObject(String json) throws Exception {
            if (json == null || json.isBlank()) return new HashMap<>();
            return MAPPER.readValue(json, new TypeReference<Map<String, Object>>() {});
        }
        static Object parse(String value) throws Exception {
            if (value == null) return null;
            String s = value;
            if (s.contains("%7B") || s.contains("%22") || s.contains("%5B")) {
                s = URLDecoder.decode(s, StandardCharsets.UTF_8);
            }
            try { return MAPPER.readValue(s, new TypeReference<Map<String, Object>>() {}); }
            catch (Exception ignore) {}
            try { return MAPPER.readValue(s, new TypeReference<List<Object>>() {}); }
            catch (Exception ignore) {}
            return s;
        }
    }
}