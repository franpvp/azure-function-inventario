package org.example.functions.funciones;

import com.azure.core.credential.AzureKeyCredential;
import com.azure.core.util.BinaryData;
import com.azure.messaging.eventgrid.EventGridEvent;
import com.azure.messaging.eventgrid.EventGridPublisherClient;
import com.azure.messaging.eventgrid.EventGridPublisherClientBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.HttpMethod;
import com.microsoft.azure.functions.HttpRequestMessage;
import com.microsoft.azure.functions.HttpResponseMessage;
import com.microsoft.azure.functions.HttpStatus;
import com.microsoft.azure.functions.annotation.AuthorizationLevel;
import com.microsoft.azure.functions.annotation.BindingName;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.HttpTrigger;
import org.example.functions.dto.InventarioDto;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class InventarioFunction {

    @FunctionName("obtenerInventarios")
    public HttpResponseMessage obtenerInventarios(
            @HttpTrigger(
                    name = "reqGetAll",
                    methods = {HttpMethod.GET},
                    authLevel = AuthorizationLevel.ANONYMOUS,
                    route = "inventarios"
            ) HttpRequestMessage<Optional<String>> request,
            final ExecutionContext context) {

        List<InventarioDto> inventarioDtos = new ArrayList<>();

        try {

            String walletPath = "/Users/franciscapalma/Desktop/Bimestre VI/Cloud Native II/Semana 3/azure-project/Wallet_DQXABCOJF1X64NFC";
            String walletEnv = System.getenv("ORACLE_WALLET_DIR");
            if (walletEnv != null && !walletEnv.isBlank()) {
                walletPath = walletEnv;
            }

            String url = "jdbc:oracle:thin:@dqxabcojf1x64nfc_tp?TNS_ADMIN=" + walletPath;
            String user = "usuario_test";
            String pass = "Usuariotest2025";

            try (Connection conn = DriverManager.getConnection(url, user, pass);
                 PreparedStatement stmt = conn.prepareStatement(
                         "SELECT ID_PRODUCTO, CANTIDAD_PRODUCTOS, ID_BODEGA FROM INVENTARIO");
                 ResultSet rs = stmt.executeQuery()) {

                while (rs.next()) {
                    inventarioDtos.add(InventarioDto.builder()
                            .idProducto(rs.getLong("ID_PRODUCTO"))
                            .cantidadProductos(rs.getInt("CANTIDAD_PRODUCTOS"))
                            .idBodega(rs.getLong("ID_BODEGA"))
                            .build());
                }
            }
        } catch (Exception e) {
            context.getLogger().severe("Error consultando Oracle: " + e.getMessage());
            return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR)
                    .header("Content-Type", "application/json")
                    .body("Error al obtener inventarios: " + e.getMessage())
                    .build();
        }

        return request.createResponseBuilder(HttpStatus.OK)
                .header("Content-Type", "application/json")
                .body(inventarioDtos)
                .build();
    }

    @FunctionName("getInventarioById")
    public HttpResponseMessage getInventarioById(
            @HttpTrigger(
                    name = "reqGetById",
                    methods = {HttpMethod.GET},
                    route = "inventarios/{id}",
                    authLevel = AuthorizationLevel.ANONYMOUS)
            HttpRequestMessage<Optional<String>> request,
            @BindingName("id") Long id,
            final ExecutionContext context) {

        final String path = request.getUri() != null ? request.getUri().getPath() : "/api/inventarios/" + id;

        String walletPath = System.getenv("ORACLE_WALLET_DIR");
        String url = "jdbc:oracle:thin:@dqxabcojf1x64nfc_tp?TNS_ADMIN=" + walletPath;
        String user = "usuario_test";
        String pass = "Usuariotest2025";

        try (Connection conn = DriverManager.getConnection(url, user, pass);
             PreparedStatement stmt = conn.prepareStatement(
                     "SELECT ID, ID_PRODUCTO, CANTIDAD_PRODUCTOS, ID_BODEGA FROM INVENTARIO WHERE ID = ?")) {

            stmt.setLong(1, id);

            try (ResultSet rs = stmt.executeQuery()) {
                if (!rs.next()) {
                    return jsonError(request, HttpStatus.NOT_FOUND, "Not Found",
                            "Inventario no encontrado con ID " + id, path);
                }

                InventarioDto dto = InventarioDto.builder()
                        .idProducto(rs.getLong("ID_PRODUCTO"))
                        .cantidadProductos(rs.getInt("CANTIDAD_PRODUCTOS"))
                        .idBodega(rs.getLong("ID_BODEGA"))
                        .build();

                return request.createResponseBuilder(HttpStatus.OK)
                        .header("Content-Type", "application/json")
                        .body(dto)
                        .build();
            }

        } catch (Exception e) {
            context.getLogger().severe("Error al obtener inventario por id: " + e.getMessage());
            return jsonError(request, HttpStatus.INTERNAL_SERVER_ERROR, "Internal Server Error",
                    "Ocurrió un error al procesar la solicitud.", path);
        }
    }


    @FunctionName("crearInventario")
    public HttpResponseMessage crearInventario(
            @HttpTrigger(
                    name = "reqPost",
                    methods = {HttpMethod.POST},
                    route = "inventarios",
                    authLevel = AuthorizationLevel.ANONYMOUS)
            HttpRequestMessage<Optional<String>> request,
            final ExecutionContext context) {

        final String eventGridTopicEndpoint = "";
        final String eventGridTopicKey      = "";

        try {
            String body = request.getBody().orElse("");
            if (body.isEmpty()) {
                return request.createResponseBuilder(HttpStatus.BAD_REQUEST)
                        .header("Content-Type", "application/json")
                        .body("El body no puede ser vacío")
                        .build();
            }

            ObjectMapper mapper = new ObjectMapper();
            InventarioDto nuevo = mapper.readValue(body, InventarioDto.class);

            String walletPath = "/Users/franciscapalma/Desktop/Bimestre VI/Cloud Native II/Semana 3/azure-project/Wallet_DQXABCOJF1X64NFC";
            String walletEnv = System.getenv("ORACLE_WALLET_DIR");
            if (walletEnv != null && !walletEnv.isBlank()) {
                walletPath = walletEnv;
            }
            String url = "jdbc:oracle:thin:@dqxabcojf1x64nfc_tp?TNS_ADMIN=" + walletPath;
            String user = "usuario_test";
            String pass = "Usuariotest2025";

            try (Connection conn = DriverManager.getConnection(url, user, pass);
                 PreparedStatement stmt = conn.prepareStatement(
                         "INSERT INTO INVENTARIO (ID_PRODUCTO, CANTIDAD_PRODUCTOS, ID_BODEGA) VALUES (?, ?, ?)")) {

                stmt.setLong(1, nuevo.getIdProducto());
                stmt.setInt(2, nuevo.getCantidadProductos());
                stmt.setLong(3, nuevo.getIdBodega());
                stmt.executeUpdate();
            }

            EventGridPublisherClient<EventGridEvent> client = new EventGridPublisherClientBuilder()
                    .endpoint(eventGridTopicEndpoint)
                    .credential(new AzureKeyCredential(eventGridTopicKey))
                    .buildEventGridEventPublisherClient();

            InventarioDto eventData = InventarioDto.builder()
                    .idProducto(nuevo.getIdProducto())
                    .cantidadProductos(nuevo.getCantidadProductos())
                    .idBodega(nuevo.getIdBodega())
                    .build();

            EventGridEvent event = new EventGridEvent(
                    "/api/inventarios",
                    "api.inventario.creado.v1",
                    BinaryData.fromObject(eventData),
                    "1.0"
            );
            event.setEventTime(OffsetDateTime.now());
            client.sendEvent(event);


            return request.createResponseBuilder(HttpStatus.CREATED)
                    .header("Content-Type", "application/json")
                    .body("Inventario creado con éxito")
                    .build();

        } catch (Exception e) {
            context.getLogger().severe("Error creando inventario: " + e.getMessage());
            return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR)
                    .header("Content-Type", "application/json")
                    .body("Error al crear inventario: " + e.getMessage())
                    .build();
        }
    }

    @FunctionName("modificarInventario")
    public HttpResponseMessage modificarInventario(
            @HttpTrigger(
                    name = "reqPut",
                    methods = {HttpMethod.PUT},
                    route = "inventarios/{id}",
                    authLevel = AuthorizationLevel.ANONYMOUS)
            HttpRequestMessage<Optional<InventarioDto>> request,
            @BindingName("id") Long id,
            final ExecutionContext context) {

        final String path = request.getUri() != null ? request.getUri().getPath() : "/api/inventarios/" + id;

        InventarioDto body = request.getBody().orElse(null);
        if (body == null) {
            return jsonError(request, HttpStatus.BAD_REQUEST, "Bad Request",
                    "Debe enviar un inventario en el body.", path);
        }
        if (body.getIdProducto() == null || body.getIdBodega() == null || body.getCantidadProductos() == null) {
            return jsonError(request, HttpStatus.BAD_REQUEST, "Bad Request",
                    "idProducto, idBodega y cantidadProductos son obligatorios.", path);
        }

        String walletPath = System.getenv("ORACLE_WALLET_DIR");
        String url = "jdbc:oracle:thin:@dqxabcojf1x64nfc_tp?TNS_ADMIN=" + walletPath;
        String user = "usuario_test";
        String pass = "Usuariotest2025";

        try (Connection conn = DriverManager.getConnection(url, user, pass)) {
            try (PreparedStatement chk = conn.prepareStatement("SELECT 1 FROM INVENTARIO WHERE ID = ?")) {
                chk.setLong(1, id);
                try (ResultSet rs = chk.executeQuery()) {
                    if (!rs.next()) {
                        return jsonError(request, HttpStatus.NOT_FOUND, "Not Found",
                                "No existe inventario con ID " + id, path);
                    }
                }
            }

            String sql = "UPDATE INVENTARIO SET ID_PRODUCTO = ?, CANTIDAD_PRODUCTOS = ?, ID_BODEGA = ? WHERE ID = ?";
            int rows;
            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setLong(1, body.getIdProducto());
                stmt.setInt(2, body.getCantidadProductos());
                stmt.setLong(3, body.getIdBodega());
                stmt.setLong(4, id);
                rows = stmt.executeUpdate();
            }

            if (rows == 0) {
                return jsonError(request, HttpStatus.NOT_FOUND, "Not Found",
                        "No se actualizó el inventario con ID " + id, path);
            }

            try {
                final String eventGridTopicEndpoint = "";
                final String eventGridTopicKey      = "";
                if (eventGridTopicEndpoint != null && eventGridTopicKey != null
                        && !eventGridTopicEndpoint.isBlank() && !eventGridTopicKey.isBlank()) {

                    EventGridPublisherClient<EventGridEvent> client =
                            new EventGridPublisherClientBuilder()
                                    .endpoint(eventGridTopicEndpoint)
                                    .credential(new AzureKeyCredential(eventGridTopicKey))
                                    .buildEventGridEventPublisherClient();

                    Map<String, Object> eventData = new LinkedHashMap<>();
                    eventData.put("id", id);
                    eventData.put("idProducto", body.getIdProducto());
                    eventData.put("idBodega", body.getIdBodega());
                    eventData.put("cantidadProductos", body.getCantidadProductos());
                    eventData.put("updatedAt", OffsetDateTime.now().toString());

                    EventGridEvent event = new EventGridEvent(
                            path,
                            "api.inventario.actualizado.v1",
                            BinaryData.fromObject(eventData),
                            "1.0"
                    );
                    event.setEventTime(OffsetDateTime.now());

                    client.sendEvent(event);
                } else {
                    context.getLogger().warning("No se publicó evento: faltan EVENTGRID_TOPIC_ENDPOINT/KEY.");
                }
            } catch (Exception egx) {
                context.getLogger().severe("Inventario actualizado pero falló publicar evento: " + egx.getMessage());
            }

            Map<String, Object> actualizado = new LinkedHashMap<>();
            actualizado.put("id", id);
            actualizado.put("idProducto", body.getIdProducto());
            actualizado.put("idBodega", body.getIdBodega());
            actualizado.put("cantidadProductos", body.getCantidadProductos());

            return request.createResponseBuilder(HttpStatus.OK)
                    .header("Content-Type", "application/json")
                    .body(actualizado)
                    .build();

        } catch (SQLException sqle) {
            final String msg = sqle.getMessage() != null ? sqle.getMessage() : "";
            if (msg.contains("ORA-17041")) {
                return jsonError(request, HttpStatus.BAD_REQUEST, "Bad Request",
                        "Parámetros incompletos o inválidos en la solicitud.", path);
            }
            if (msg.contains("ORA-02291")) {
                return jsonError(request, HttpStatus.BAD_REQUEST, "Bad Request",
                        "El producto o la bodega no existen (violación de clave foránea).", path);
            }
            if (msg.contains("ORA-00001")) {
                return jsonError(request, HttpStatus.CONFLICT, "Conflict",
                        "Duplicidad en combinación producto-bodega.", path);
            }
            context.getLogger().severe("SQL error modificarInventario: " + msg);
            return jsonError(request, HttpStatus.INTERNAL_SERVER_ERROR, "Internal Server Error",
                    "Ocurrió un error al procesar la solicitud.", path);
        } catch (Exception ex) {
            context.getLogger().severe("Error modificarInventario: " + ex.getClass().getName());
            return jsonError(request, HttpStatus.INTERNAL_SERVER_ERROR, "Internal Server Error",
                    "Ocurrió un error al procesar la solicitud.", path);
        }
    }


    @FunctionName("eliminarInventario")
    public HttpResponseMessage eliminarInventario(
            @HttpTrigger(
                    name = "reqDelete",
                    methods = {HttpMethod.DELETE},
                    route = "inventarios/{id}",
                    authLevel = AuthorizationLevel.ANONYMOUS)
            HttpRequestMessage<Optional<String>> request,
            @BindingName("id") Long id,
            final ExecutionContext context) {

        String walletPath = System.getenv("ORACLE_WALLET_DIR");
        String url = "jdbc:oracle:thin:@dqxabcojf1x64nfc_tp?TNS_ADMIN=" + walletPath;
        String user = "usuario_test";
        String pass = "Usuariotest2025";

        final String eventGridTopicEndpoint = "";
        final String eventGridTopicKey      = "";

        Map<String, Object> before = null;
        int rows;
        try (Connection conn = DriverManager.getConnection(url, user, pass)) {

            try (PreparedStatement sel = conn.prepareStatement(
                    "SELECT ID, ID_PRODUCTO, CANTIDAD_PRODUCTOS, ID_BODEGA FROM INVENTARIO WHERE ID=?")) {
                sel.setLong(1, id);
                try (ResultSet rs = sel.executeQuery()) {
                    if (!rs.next()) {
                        return request.createResponseBuilder(HttpStatus.NOT_FOUND)
                                .header("Content-Type", "application/json")
                                .body("{\"message\":\"No existe inventario con ID " + id + "\"}")
                                .build();
                    }
                    before = new LinkedHashMap<>();
                    before.put("id", rs.getLong("ID"));
                    before.put("idProducto", rs.getLong("ID_PRODUCTO"));
                    before.put("cantidadProductos", rs.getInt("CANTIDAD_PRODUCTOS"));
                    before.put("idBodega", rs.getLong("ID_BODEGA"));
                }
            }

            try (PreparedStatement stmt = conn.prepareStatement("DELETE FROM INVENTARIO WHERE ID=?")) {
                stmt.setLong(1, id);
                rows = stmt.executeUpdate();
            }

        } catch (Exception e) {
            context.getLogger().severe("Error al eliminar inventario: " + e.getMessage());
            return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR)
                    .header("Content-Type", "application/json")
                    .body("{\"error\":\"" + e.getMessage().replace("\"", "\\\"") + "\"}")
                    .build();
        }

        if (rows == 0) {
            return request.createResponseBuilder(HttpStatus.NOT_FOUND)
                    .header("Content-Type", "application/json")
                    .body("{\"message\":\"No existe inventario con ID " + id + "\"}")
                    .build();
        }

        try {
            if (eventGridTopicEndpoint != null && !eventGridTopicEndpoint.isBlank() && eventGridTopicKey != null && !eventGridTopicKey.isBlank()) {
                EventGridPublisherClient<EventGridEvent> client = new EventGridPublisherClientBuilder()
                        .endpoint(eventGridTopicEndpoint)
                        .credential(new AzureKeyCredential(eventGridTopicKey))
                        .buildEventGridEventPublisherClient();

                Map<String, Object> eventData = new LinkedHashMap<>(before);
                eventData.put("deletedAt", OffsetDateTime.now().toString());

                EventGridEvent event = new EventGridEvent(
                        "/api/inventarios/" + id,
                        "api.inventario.eliminado.v1",
                        BinaryData.fromObject(eventData),
                        "1.0"
                );
                event.setEventTime(OffsetDateTime.now());

                client.sendEvent(event);
            } else {
                context.getLogger().warning("No se publicó evento: faltan EVENTGRID_TOPIC_ENDPOINT/KEY.");
            }
        } catch (Exception egx) {
            context.getLogger().severe("Inventario eliminado pero falló publicar evento: " + egx.getMessage());
        }

        return request.createResponseBuilder(HttpStatus.OK)
                .header("Content-Type", "application/json")
                .body("{\"message\":\"Inventario eliminado con éxito\",\"id\":" + id + "}")
                .build();
    }

    private HttpResponseMessage jsonError(HttpRequestMessage<?> request,
                                          HttpStatus status,
                                          String error,
                                          String message,
                                          String path) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("timestamp", java.time.OffsetDateTime.now().toString());
        body.put("status", status.value());
        body.put("error", error);
        body.put("message", message);
        body.put("path", path);

        return request.createResponseBuilder(status)
                .header("Content-Type", "application/json")
                .body(body)
                .build();
    }

}
