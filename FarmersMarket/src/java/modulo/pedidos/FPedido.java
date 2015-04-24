/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package modulo.pedidos;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import modulo.ofertas.FOferta;
import modulo.ofertas.dao.OfertaDao;
import modulo.ofertas.dto.OfertaDto;
import modulo.pedidos.dao.CarritoDao;
import modulo.pedidos.dao.InventarioDao;
import modulo.pedidos.dao.PedidoDao;
import modulo.pedidos.dto.CarritoDto;
import modulo.pedidos.dto.InventarioDto;
import modulo.pedidos.dto.PedidoDto;
import utilidades.Conexion;
import utilidades.Exceptions;

/**
 *
 * @author User
 */
public class FPedido {

    //Conexion
    Connection miConexion = null;

    //Clases DataAccessObject (DAO)
    PedidoDao peDao = null;
    InventarioDao inDao = null;
    OfertaDao ofDao = null;
    CarritoDao carDao = null;
    //Clases DataTransferObject (DTO)
    PedidoDto peDto = null;
    InventarioDto inDto = null;
    OfertaDto ofDto = null;
    CarritoDto carDto = null;

    public FPedido() {
        this.miConexion = Conexion.getInstance();
        //Clases DataAccessObject (DAO)
        this.peDao = new PedidoDao();
        this.inDao = new InventarioDao();
        this.ofDao = new OfertaDao();
        this.carDao = new CarritoDao();
        //Clases DataTransferObject (DTO)
        this.peDto = new PedidoDto();
        this.inDto = new InventarioDto();
        this.ofDto = new OfertaDto();
        this.carDto = new CarritoDto();
    }

    public List obtenerCarrito(long idCliente) {
        return carDao.obtenerCarrito(idCliente, miConexion);
    }

    public int crearPedido(long idUsuario, int idOferta, int cantidad) {
        return peDao.crearPedido(idUsuario, idOferta, cantidad, miConexion);
    }

    public String eliminarDelCarrito(int idOferta) {
        return carDao.eliminarDelCarrito(idOferta, miConexion);
    }

    public String activarPedido(int idPedido) {
        return peDao.activarPedido(idPedido, miConexion);
    }

    public String descontarInventario(ArrayList<CarritoDto> carrito) {
        return inDao.descontarInventario(carrito, miConexion);
    }

    public String actulizarCantidad(int cantidad, int idOferta, int idPedido) {
        return carDao.actualizarCantidad(cantidad, idOferta, idPedido, miConexion);
    }

    public String calcularTotalPedido(int idPedido) {
        return peDao.calcularTotalPedido(idPedido, miConexion);
    }

    public List obtenerPedidosCliente(long idCliente) {
        return peDao.obtenerPedidosCliente(idCliente, miConexion);
    }

    public List obtenerDetallePedidosCliente(int idPedido) {
        return peDao.obtenerDetallePedidosCliente(idPedido, miConexion);
    }

    public List obtenerPedidosProductor(long idProductor) {
        return peDao.obtenerPedidosProductor(idProductor, miConexion);
    }

    public List obtenerDetallePedidosProductor(int idPedido) {
        return peDao.obtenerDetallePedidosProductor(idPedido, miConexion);
    }

    public String cancelarPedido(int idPedido, ArrayList<CarritoDto> a) {
        return inDao.cancelarPedido(idPedido, a, miConexion);
    }

    public int obtenerEstadoPedido(int idPedido) {
        return peDao.obtenerEstadoPedido(idPedido, miConexion);
    }

    
}
