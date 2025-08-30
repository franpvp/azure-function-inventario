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
import java.util.ArrayList;
import java.util.List;
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

        String walletPath = System.getenv("ORACLE_WALLET_DIR");
        String url = "jdbc:oracle:thin:@dqxabcojf1x64nfc_tp?TNS_ADMIN=" + walletPath;
        String user = "usuario_test";
        String pass = "Usuariotest2025";

        InventarioDto inventarioDto = null;
        try (Connection conn = DriverManager.getConnection(url, user, pass);
             PreparedStatement stmt = conn.prepareStatement("SELECT * FROM INVENTARIO WHERE ID = ?")) {
            stmt.setLong(1, id);
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                inventarioDto = InventarioDto.builder()
                        .idProducto(rs.getLong("ID"))
                        .cantidadProductos(rs.getInt("CANTIDAD_PRPDUCTOS"))
                        .idBodega(rs.getLong("ID_BODEGA"))
                        .build();
            }
        } catch (Exception e) {
            context.getLogger().severe("Error al obtener inventario: " + e.getMessage());
            return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR)
                    .header("Content-Type", "application/json")
                    .body("Error: " + e.getMessage())
                    .build();
        }

        if (inventarioDto == null) {
            return request.createResponseBuilder(HttpStatus.NOT_FOUND)
                    .header("Content-Type", "application/json")
                    .body("Inventario no encontrado con ID " + id)
                    .build();
        }
        return request.createResponseBuilder(HttpStatus.OK)
                .header("Content-Type", "application/json")
                .body(inventarioDto)
                .build();
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

            // conexión a Oracle
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

        InventarioDto actualizado = request.getBody().orElse(null);
        if (actualizado == null) {
            return request.createResponseBuilder(HttpStatus.BAD_REQUEST)
                    .body("Debe enviar un inventario en el body")
                    .build();
        }

        String walletPath = System.getenv("ORACLE_WALLET_DIR");
        String url = "jdbc:oracle:thin:@dqxabcojf1x64nfc_tp?TNS_ADMIN=" + walletPath;
        String user = "usuario_test";
        String pass = "Usuariotest2025";

        int rows;
        try (Connection conn = DriverManager.getConnection(url, user, pass);
             PreparedStatement stmt = conn.prepareStatement(
                     "UPDATE INVENTARIO SET ID_PRODUCTO=?, CANTIDAD_PRODUCTOS=?, ID_BODEGA=? WHERE ID=?")) {
            stmt.setLong(1, actualizado.getIdProducto());
            stmt.setInt(2, actualizado.getCantidadProductos());
            stmt.setLong(3, actualizado.getIdBodega());
            rows = stmt.executeUpdate();
        } catch (Exception e) {
            context.getLogger().severe("Error al actualizar inventario: " + e.getMessage());
            return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Error: " + e.getMessage()).build();
        }

        if (rows == 0) {
            return request.createResponseBuilder(HttpStatus.NOT_FOUND)
                    .body("No existe inventario con ID " + id).build();
        }

        return request.createResponseBuilder(HttpStatus.OK)
                .body("Inventario actualizado con éxito").build();
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

}
