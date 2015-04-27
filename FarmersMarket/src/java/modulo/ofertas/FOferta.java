package modulo.ofertas;

import modulo.ofertas.dao.CategoriaDao;
import modulo.ofertas.dao.OfertaDao;
import modulo.ofertas.dao.ProductoAsociadoDao;
import modulo.ofertas.dao.ProductoDao;
import modulo.ofertas.dto.CategoriaDto;
import modulo.ofertas.dto.OfertaDto;
import modulo.ofertas.dto.ProductoAsociadoDto;
import modulo.ofertas.dto.ProductoDto;
import modulo.ofertas.dto.PromocionDto;
import utilidades.Conexion;
import java.sql.Connection;
import java.util.List;
import modulo.ofertas.dao.PresentacionDao;
import modulo.ofertas.dao.PromocionDao;
import modulo.ofertas.dto.PresentacionDto;
import modulo.pedidos.dao.InventarioDao;
import modulo.pedidos.dto.InventarioDto;
import utilidades.Exceptions;

/**
 * @author kennross
 */
public class FOferta {

    //Conexion
    Connection miConexion = null;

    //Clases DataAccessObject (DAO)
    OfertaDao ofDao = null;
    ProductoDao pDao = null;
    CategoriaDao caDao = null;
    ProductoAsociadoDao paDao = null;
    PromocionDao promoDao = null;
    PresentacionDao preDao = null;
    InventarioDao inDao = null;
    //Clases DataTransferObject (DTO)
    OfertaDto ofDto = null;
    ProductoDto pDto = null;
    ProductoAsociadoDto paDto = null;
    PromocionDto promoDto = null;
    CategoriaDto caDto = null;
    PresentacionDto preDto = null;
    InventarioDto inDto = null;
    
    public FOferta() {
        //Instancio la conexion
        this.miConexion = Conexion.getInstance();

        this.ofDao = new OfertaDao();
        this.pDao = new ProductoDao();
        this.caDao = new CategoriaDao();
        this.paDao = new ProductoAsociadoDao();
        this.promoDao = new PromocionDao();
        this.preDao = new PresentacionDao();
        this.inDao = new InventarioDao();

        this.ofDto = new OfertaDto();
        this.pDto = new ProductoDto();
        this.promoDto = new PromocionDto();
        this.caDto = new CategoriaDto();
        this.paDto = new ProductoAsociadoDto();
        this.preDto = new PresentacionDto();
        this.inDto = new InventarioDto();
    }

    //Insert's a la base de datos directamente
    public String asociarProductos(long idProductor, int[] idProducto) {
        return paDao.insertProductoAsociado(idProductor, idProducto, miConexion);
    }

    //Consultas Por Un Criterio (ProductosAsociados)
    public int obtenerIdPaPorIds(long idProductor, int idProducto) {
        return paDao.obtenerIdPA(idProductor, idProducto, miConexion);
    }

    //Consultas Por Un Criterio (Produtos)
    public List obtenerTodosLosProductos(long idUsuario) {
        return pDao.obtenerProductos(idUsuario, miConexion);
    }

    public String obtenerNumeroDeProductosAsociadosPorProducto(long idProductor) {
        return paDao.obtenerNumeroProductosAsociados(idProductor, miConexion);
    }

    public List validarProductoYaAsociado(long idProductor, int[] idProducto) {
        return paDao.validarYaProdAso(idProductor, idProducto, miConexion);
    }

    public List obtenerProductosAsociados(long idProductor) {
        return pDao.obtenerProductosPorId(idProductor, miConexion);
    }

    public ProductoDto obtenerProductoConIdProductoAso(int idProductoAso) {
        return pDao.obtenerProductoIdProducoAso(idProductoAso, miConexion);
    }

    //Consultas Por Un Criterio (Categorias)
    public String obtenerNombreDeCategoriaPorId(int idCategoria) {
        return caDao.obtenerNombrePorId(idCategoria, miConexion);
    }

    //Eliminación de registros directamente
    public String eliminarAsociasionDeProducto(int idProductoAso) {
        return paDao.eliminarUnProductoAsociado(idProductoAso, miConexion);
    }

    public List obtenerOfertas(long idUsuario, String producto, int categoria, int presentacion) throws Exceptions {
        return ofDao.obtenerOfertas(idUsuario, producto, categoria, presentacion, miConexion);
    }

    public OfertaDto obtenerOfertaPorId(int idOferta) {
        return ofDao.obtenerOfertaPorId(idOferta, miConexion);
    }

    public String actualizarCantidad(int idOferta) {
        return ofDao.actualizarCantidad(idOferta, miConexion);
    }

    public List obtenerPresentaciones(int idCategoria) {
        return preDao.obtenerPresentaciones(idCategoria, miConexion);
    }

    public List obtenerPromociones() {
        return promoDao.obtenerPromociones(miConexion);
    }

    public int ofertarProducto(OfertaDto ofDto) {
        return ofDao.ofertarProducto(ofDto, miConexion);
    }

    public int obtenerNumeroOfertasConProducto(int idProAso, long idUsuario) {
        return ofDao.obtenerNumeroOfertasConProducto(idProAso, idUsuario, miConexion);
    }

    public int obtenerProductoOfertado(int idProAso) {
        return ofDao.obtenerProductoOfertado(idProAso, miConexion);
    }
    public List obtenerOfertasPorProductor(long idUsuario) {
        return ofDao.obtenerOfertasPorProductor(idUsuario, miConexion);
    }
    
    public String abastecerInventario(int cantidad, int idOferta){
        return inDao.abastecerInventario(cantidad, idOferta, miConexion);
    }
    
}
