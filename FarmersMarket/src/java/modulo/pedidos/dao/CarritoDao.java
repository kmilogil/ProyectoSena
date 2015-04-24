/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package modulo.pedidos.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import modulo.pedidos.dto.CarritoDto;

/**
 *
 * @author User
 */
public class CarritoDao {

    String mensaje = "";
    PreparedStatement pstm = null;
    ResultSet rs = null;
    int rtdo = 0;

    public List obtenerCarrito(long idCliente, Connection unaConexion) {
        ArrayList<CarritoDto> carritos = new ArrayList();
        try {
            pstm = unaConexion.prepareStatement("select pe.idPedido,o.idOferta, p.nombres as producto, o.precioVenta as precio, c.cantidad as cantidadPedida"
                    + ", i.cantidad as cantidadDisponible from pedidos as pe \n"
                    + "join usuarios as u on u.idUsuario = pe.idCliente\n"
                    + "join carrito as c on c.idPedido = pe.idPedido \n"
                    + "join ofertas as o on c.idOferta = o.idOferta  \n"
                    + "join productoasociado as pa on o.idProdAsoc = pa.idProdAsoc \n"
                    + "join productos as p on pa.idProducto = p.idProducto\n"
                    + "join inventario as i on o.idOferta = i.idOferta \n"
                    + " where pe.idCliente = ? and pe.estadoPedido = 1 and i.cantidad > 0;");
            pstm.setLong(1, idCliente);
            rs = pstm.executeQuery();
            while (rs.next()) {
                CarritoDto carrito = new CarritoDto();
                carrito.getOfDto().getProAso().getProDto().setNombres(rs.getString("producto"));
                carrito.setIdOferta(rs.getInt("idOferta"));
                carrito.getOfDto().setPrecioVenta(rs.getLong("precio"));
                carrito.setCantidad(rs.getInt("cantidadPedida"));
                carrito.getOfDto().getInDto().setCantidad(rs.getLong("cantidadDisponible"));
                carrito.setIdPedido(rs.getInt("idPedido"));
                carritos.add(carrito);
            }
        } catch (SQLException ex) {
            ex.getMessage();
        }

        return carritos;
    }
    public String eliminarDelCarrito(int idOferta,Connection unaConexion){
        try{
            pstm = unaConexion.prepareStatement("delete from carrito where idOferta = ?");
            pstm.setInt(1, idOferta);
            rtdo = pstm.executeUpdate();
            if (rtdo != 0) {
                mensaje = "ok";
            }else{
                mensaje = "okno";
            }
        }catch (SQLException ex) {
            ex.getMessage();
        }
        return mensaje;
    }
    
    public String actualizarCantidad(int cantidad,int idOferta,int idPedido, Connection unaConexion){
        String salida;
        try{
            pstm = unaConexion.prepareStatement("update carrito set cantidad = ? where idOferta = ? and idPedido = ?;");
            pstm.setInt(1, cantidad);
            pstm.setInt(2, idOferta);
            pstm.setInt(3, idPedido);
            rtdo = pstm.executeUpdate();
            if (rtdo != 0) {
                salida = "ok";
            }else{
                salida = "no";
            }
        }catch (SQLException ex) {
            salida = ex.getMessage();
        }
        return salida;
    }
}
