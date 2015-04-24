/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package modulo.pedidos.dao;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import modulo.pedidos.dto.CarritoDto;
import modulo.pedidos.dto.PedidoDto;
import utilidades.Exceptions;

/**
 *
 * @author User
 */
public class PedidoDao {

    String mensaje = "";
    PreparedStatement pstm = null;
    CallableStatement ctm = null;
    ResultSet rs = null;
    int rtdo = 0;

    public int crearPedido(long idUsuario, int idOferta, int cantidad, Connection unaConexion) {
        try {
            ctm = unaConexion.prepareCall("call sp_pedidoCarrito(?,?,?,?)");
            ctm.setLong(1, idUsuario);
            ctm.setInt(2, idOferta);
            ctm.setInt(3, cantidad);
            ctm.registerOutParameter(4, Types.INTEGER);
            ctm.execute();
            rtdo = ctm.getInt(4);
        } catch (SQLException ex) {
            ex.getMessage();
        }
        return rtdo;
    }

    public String activarPedido(int idPedido, Connection unaConexion) {
        try {
            pstm = unaConexion.prepareStatement("update pedidos set estadoPedido = 2, fechaPedido = curdate(), fechaEntrega = date_add(curdate(), interval 4 day) where idPedido = ?;");
            pstm.setInt(1, idPedido);
            rtdo = pstm.executeUpdate();
            if (rtdo != 0) {
                mensaje = "ok";
            } else {
                mensaje = "okno";
            }
        } catch (SQLException ex) {
            mensaje = ex.getMessage();
        }
        return mensaje;
    }

    public String calcularTotalPedido(int idPedido, Connection unaConexion) {
        String total = "";
        try {
            pstm = unaConexion.prepareStatement("select ceil((sum(c.cantidad * o.precioVenta) - sum((c.cantidad * o.precioVenta)*p.detalle))) as total \n"
                    + "from carrito c\n"
                    + "join pedidos pe on c.idPedido = pe.idPedido\n"
                    + "join ofertas o on c.idOferta = o.idOferta\n"
                    + "join promociones p on o.idPromocion = p.idPromocion\n"
                    + "where pe.idPedido = ? ;");
            pstm.setLong(1, idPedido);
            rs = pstm.executeQuery();
            while (rs.next()) {
                total = rs.getString("total");
            }
        } catch (SQLException ex) {
            mensaje = ex.getMessage();
        }
        return total;
    }

    public List obtenerPedidosCliente(long idCliente, Connection unaConexion) {
        ArrayList<PedidoDto> pedidos = new ArrayList();
        try {
            pstm = unaConexion.prepareStatement("select pe.idPedido ,pe.fechaEntrega as entrega, pe.estadoPedido as estado"
                    + ", ceil((sum(c.cantidad * o.precioVenta)  - sum((c.cantidad * o.precioVenta)*p.detalle))) as 'total'\n"
                    + "from carrito c\n"
                    + "join pedidos pe on c.idPedido = pe.idPedido\n"
                    + "join ofertas o on c.idOferta = o.idOferta\n"
                    + "join promociones p on o.idPromocion = p.idPromocion\n"
                    + "where pe.idCliente = ? and pe.idPedido not in (select ped.idPedido from pedidos as ped where ped.estadoPedido = 1) group by pe.idPedido;");
            pstm.setLong(1, idCliente);
            rs = pstm.executeQuery();
            while (rs.next()) {
                PedidoDto pedido = new PedidoDto();
                pedido.setIdPedido(rs.getInt("idPedido"));
                pedido.setFechaEntrega(rs.getString("entrega"));
                pedido.setEstadoPedido(rs.getInt("estado"));
                pedido.setTotal(rs.getLong("total"));
                pedidos.add(pedido);
            }
        } catch (SQLException ex) {
            ex.getMessage();
        }
        return pedidos;
    }

    public List obtenerDetallePedidosCliente(int idPedido, Connection unaConexion) {
        ArrayList<CarritoDto> pedidos = new ArrayList();
        try {
            pstm = unaConexion.prepareStatement("select o.idOferta, p.nombres as producto, pre.descripcion as unidad, ca.cantidad as cantidad \n"
                    + "from pedidos as pe join carrito as ca on pe.idPedido = ca.idPedido\n"
                    + "join ofertas as o on ca.idOferta = o.idOferta \n"
                    + "join productoasociado as pa on o.idProdAsoc = pa.idProdAsoc\n"
                    + "join productos as p on pa.idProducto = p.idProducto\n"
                    + "join presentaciones as pre on o.idPresentacion = pre.idPresentacion\n"
                    + "where pe.idPedido = ? group by o.idOferta;");
            pstm.setInt(1, idPedido);
            rs = pstm.executeQuery();
            while (rs.next()) {
                CarritoDto pedido = new CarritoDto();
                pedido.getOfDto().getProAso().getProDto().setNombres(rs.getString("producto"));
                pedido.getOfDto().getPreDto().setDescripcion(rs.getString("unidad"));
                pedido.setCantidad(rs.getInt("cantidad"));
                pedido.setIdOferta(rs.getInt("idOferta"));
                pedidos.add(pedido);
            }
        } catch (SQLException ex) {
            ex.getMessage();
        }
        return pedidos;
    }

    public List obtenerPedidosProductor(long idProductor, Connection unaConexion) {
        ArrayList<PedidoDto> pedidos = new ArrayList();
        try {
            pstm = unaConexion.prepareStatement("select concat(u.nombres, ' ' ,u.apellidos) as cliente, pe.idPedido ,pe.fechaEntrega as entrega, pe.estadoPedido as estado, ceil((sum(c.cantidad * o.precioVenta)  - sum((c.cantidad * o.precioVenta)*p.detalle))) as 'total'\n"
                    + "from carrito c\n"
                    + "join pedidos pe on c.idPedido = pe.idPedido\n"
                    + "join usuarios as u on u.idUsuario = pe.idCliente\n"
                    + "join ofertas o on c.idOferta = o.idOferta\n"
                    + "join productoasociado as pa on pa.idProdAsoc = o.idProdAsoc\n"
                    + "join usuarios as us on us.idUsuario = pa.idUsuario\n"
                    + "join promociones p on o.idPromocion = p.idPromocion\n"
                    + "where us.idUsuario = ? and pe.idPedido not in (select ped.idPedido from pedidos as ped where ped.estadoPedido = 1) "
                    + "group by pe.idPedido;");
            pstm.setLong(1, idProductor);
            rs = pstm.executeQuery();
            while (rs.next()) {
                PedidoDto pedido = new PedidoDto();
                pedido.setIdPedido(rs.getInt("idPedido"));
                pedido.setFechaEntrega(rs.getString("entrega"));
                pedido.setEstadoPedido(rs.getInt("estado"));
                pedido.setTotal(rs.getLong("total"));
                pedido.getUsDto().setNombres(rs.getString("cliente"));
                pedidos.add(pedido);
            }
        } catch (SQLException ex) {
            ex.getMessage();
        }

        return pedidos;
    }

    public List obtenerDetallePedidosProductor(int idPedido, Connection unaConexion) {
        ArrayList<CarritoDto> pedidos = new ArrayList();
        try {
            pstm = unaConexion.prepareStatement("select u.direccion, ci.Nombre as ciudad, p.nombres as producto, pre.descripcion as unidad, ca.cantidad as cantidad \n"
                    + "from pedidos as pe join carrito as ca on pe.idPedido = ca.idPedido\n"
                    + "join usuarios as u on u.idUsuario = pe.idCliente\n"
                    + "join ciudades as ci on ci.idCiudad = u.idCiudad\n"
                    + "join ofertas as o on ca.idOferta = o.idOferta \n"
                    + "join productoasociado as pa on o.idProdAsoc = pa.idProdAsoc\n"
                    + "join productos as p on pa.idProducto = p.idProducto\n"
                    + "join presentaciones as pre on o.idPresentacion = pre.idPresentacion\n"
                    + "where pe.idPedido = ? group by o.idOferta;");
            pstm.setLong(1, idPedido);
            rs = pstm.executeQuery();
            while (rs.next()) {
                CarritoDto pedido = new CarritoDto();
                pedido.getPedDto().getUsDto().setDireccion(rs.getString("direccion"));
                pedido.getPedDto().getUsDto().getCiDto().setNombre(rs.getString("ciudad"));
                pedido.getOfDto().getProAso().getProDto().setNombres(rs.getString("producto"));
                pedido.getOfDto().getPreDto().setDescripcion("unidad");
                pedido.setCantidad(rs.getInt("cantidad"));
                pedidos.add(pedido);
            }
        } catch (SQLException ex) {
            ex.getMessage();
        }
        return pedidos;
    }

    public int obtenerEstadoPedido(int idPedido, Connection unaConexion) {
        int estado = 0;
        try {
            pstm = unaConexion.prepareStatement("select estadoPedido from pedidos where idPedido = ?");
            pstm.setInt(1, idPedido);
            rs = pstm.executeQuery();
            while (rs.next()) {
                estado = rs.getInt("estadoPedido");
            }
        } catch (SQLException ex) {
            ex.getMessage();
        }
        return estado;
    }
    

}
