package org.example.functions.funciones;

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

            // 2) Conectar usando TNS_ADMIN hacia tu alias _tp del tnsnames.ora
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

                // Ajusta los setters según tu InventarioDto (si tienes campo 'id', mapea también).
                InventarioDto dto = InventarioDto.builder()
                        //.id(rs.getLong("ID"))                        // <- descomenta si tu DTO tiene 'id'
                        .idProducto(rs.getLong("ID_PRODUCTO"))
                        .cantidadProductos(rs.getInt("CANTIDAD_PRODUCTOS")) // ojo al nombre correcto
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

        // 1) Validación básica del body
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
            // 2) Verificar existencia del inventario por ID
            try (PreparedStatement chk = conn.prepareStatement("SELECT 1 FROM INVENTARIO WHERE ID = ?")) {
                chk.setLong(1, id);
                try (ResultSet rs = chk.executeQuery()) {
                    if (!rs.next()) {
                        return jsonError(request, HttpStatus.NOT_FOUND, "Not Found",
                                "No existe inventario con ID " + id, path);
                    }
                }
            }

            // 3) Ejecutar UPDATE (OJO con los 4 placeholders)
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

            // 4) Devolver el recurso actualizado (200 OK)
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
            // Mapeos útiles de Oracle -> HTTP:
            if (msg.contains("ORA-17041")) { // Missing IN or OUT parameter
                return jsonError(request, HttpStatus.BAD_REQUEST, "Bad Request",
                        "Parámetros incompletos o inválidos en la solicitud.", path);
            }
            if (msg.contains("ORA-02291")) { // FK hijo sin padre (violación FK)
                return jsonError(request, HttpStatus.BAD_REQUEST, "Bad Request",
                        "El producto o la bodega no existen (violación de clave foránea).", path);
            }
            if (msg.contains("ORA-00001")) { // unique constraint violated
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

        int rows;
        try (Connection conn = DriverManager.getConnection(url, user, pass);
             PreparedStatement stmt = conn.prepareStatement("DELETE FROM INVENTARIO WHERE ID=?")) {
            stmt.setLong(1, id);
            rows = stmt.executeUpdate();
        } catch (Exception e) {
            context.getLogger().severe("Error al eliminar inventario: " + e.getMessage());
            return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Error: " + e.getMessage()).build();
        }

        if (rows == 0) {
            return request.createResponseBuilder(HttpStatus.NOT_FOUND)
                    .body("No existe inventario con ID " + id).build();
        }

        return request.createResponseBuilder(HttpStatus.OK)
                .body("Inventario eliminado con éxito").build();
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
