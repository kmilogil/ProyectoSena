package modulo.ofertas.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import modulo.ofertas.dto.ProductoDto;

/**
 *
 * @author kennross
 */
public class ProductoDao {

    PreparedStatement pstm = null;
    int rtdo;
    ResultSet rs = null;
    String mensaje = "";
    String sqlTemp = "";

    public List obtenerProductos(long idUsuario, Connection unaConexion) {
        ArrayList<ProductoDto> todosProductos = null;
        sqlTemp = "select p.idProducto, p.nombres as 'Producto', c.descripcion as 'Categoria', p.imagen as imagen from productos p\n"
                + "join categorias c on c.idCategoria = p.idCategoria\n"
                + "where p.idProducto not in(select idProducto from productoAsociado where idUsuario = ?);";        
        try {
            pstm = unaConexion.prepareStatement(sqlTemp);
            pstm.setLong(1, idUsuario);
            rs = pstm.executeQuery();

            todosProductos = new ArrayList();
            while (rs.next()) {
                ProductoDto productoTemp = new ProductoDto();
                productoTemp.setIdProducto(rs.getInt("idProducto"));
                productoTemp.setNombres(rs.getString("Producto"));
                productoTemp.getCatDto().setDescripcion(rs.getString("Categoria"));
                productoTemp.setImagen(rs.getString("imagen"));
                todosProductos.add(productoTemp);
            }
        } catch (SQLException ex) {
            System.out.println("Error, detalle: " + ex.getMessage());
        }
        return todosProductos;
    }

    public List obtenerProductosPorId(long idUsuario, Connection unaConexion) {
        ArrayList<ProductoDto> misProductos = null;
        sqlTemp = "SELECT p.idProducto, p.Nombres, p.idCategoria, p.Imagen FROM productos as p "
                + "JOIN productoasociado as pa on (p.idProducto = pa.idProducto) "
                + "JOIN usuarios as u on (u.idUsuario = pa.idUsuario) WHERE u.idUsuario = ?";
        try {
            pstm = unaConexion.prepareStatement(sqlTemp);
            pstm.setLong(1, idUsuario);
            rs = pstm.executeQuery();

            misProductos = new ArrayList();
            while (rs.next()) {
                ProductoDto productoTemp = new ProductoDto();
                productoTemp.setIdProducto(rs.getInt("idProducto"));
                productoTemp.setNombres(rs.getString("Nombres"));
                productoTemp.setIdCategoria(rs.getInt("idCategoria"));
                productoTemp.setImagen(rs.getString("Imagen"));
                misProductos.add(productoTemp);
            }
        } catch (SQLException ex) {
            System.out.println("Error, detalle: " + ex.getMessage());
        }
        return misProductos;
    }

    public ProductoDto obtenerProductoIdProducoAso(int idProductoAso, Connection unaConexion) {
        ProductoDto productoTemp = null;
        sqlTemp = "SELECT p.idProducto, Nombres, idCategoria, Imagen FROM productos as p "
                + "JOIN productoasociado as pa on (p.idProducto = pa.idProducto) WHERE idProdAsoc = ?";
        try {
            pstm = unaConexion.prepareStatement(sqlTemp);
            pstm.setLong(1, idProductoAso);
            rs = pstm.executeQuery();

            while (rs.next()) {
                productoTemp = new ProductoDto();
                productoTemp.setIdProducto(rs.getInt("idProducto"));
                productoTemp.setNombres(rs.getString("Nombres"));
                productoTemp.setIdCategoria(rs.getInt("idCategoria"));
                productoTemp.setImagen(rs.getString("Imagen"));
            }
        } catch (SQLException ex) {
            System.out.println("Error, detalle: " + ex.getMessage());
        }
        return productoTemp;
    }
}
