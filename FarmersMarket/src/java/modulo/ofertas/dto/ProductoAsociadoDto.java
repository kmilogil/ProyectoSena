package modulo.ofertas.dto;

import modulo.usuarios.dto.UsuarioDto;



/**
 *
 * @author kennross
 */
public class ProductoAsociadoDto {

    private int idProdAsoc = 0;
    private long idUsuario = 0;
    private int idProducto = 0;
    private UsuarioDto usDto = new UsuarioDto();
    private ProductoDto proDto = new ProductoDto();

    @Override
    public String toString() {
        return "ProductoAsociadoDto{" + "idProdAsoc=" + idProdAsoc + ", idUsuario=" + idUsuario + ", idProducto=" + idProducto + ", usDto=" + usDto + ", proDto=" + proDto + '}';
    }
   

    /**
     * @return the idProdAsoc
     */
    public int getIdProdAsoc() {
        return idProdAsoc;
    }

    /**
     * @param idProdAsoc the idProdAsoc to set
     */
    public void setIdProdAsoc(int idProdAsoc) {
        this.idProdAsoc = idProdAsoc;
    }

    /**
     * @return the idUsuario
     */
    public long getIdUsuario() {
        return idUsuario;
    }

    /**
     * @param idUsuario the idUsuario to set
     */
    public void setIdUsuario(long idUsuario) {
        this.idUsuario = idUsuario;
    }

    /**
     * @return the idProducto
     */
    public int getIdProducto() {
        return idProducto;
    }

    /**
     * @param idProducto the idProducto to set
     */
    public void setIdProducto(int idProducto) {
        this.idProducto = idProducto;
    }

    /**
     * @return the usDto
     */
    public UsuarioDto getUsDto() {
        return usDto;
    }

    /**
     * @param usDto the usDto to set
     */
    public void setUsDto(UsuarioDto usDto) {
        this.usDto = usDto;
    }

    /**
     * @return the proDto
     */
    public ProductoDto getProDto() {
        return proDto;
    }

    /**
     * @param proDto the proDto to set
     */
    public void setProDto(ProductoDto proDto) {
        this.proDto = proDto;
    }

}
