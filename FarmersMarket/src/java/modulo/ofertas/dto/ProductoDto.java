package modulo.ofertas.dto;

/**
 *
 * @author kennross
 */
public class ProductoDto {

    private int idProducto = 0;
    private String nombres = "";
    private String unidad = "";
    private int idCategoria = 0;
    private String imagen = "";
    private CategoriaDto catDto = new CategoriaDto();

    @Override
    public String toString() {
        return "ProductoDto{" + "idProducto=" + idProducto + ", nombres=" + nombres + ", unidad=" + unidad + ", idCategoria=" + idCategoria + ", imagen=" + imagen + ", catDto=" + catDto + '}';
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
     * @return the nombres
     */
    public String getNombres() {
        return nombres;
    }

    /**
     * @param nombres the nombres to set
     */
    public void setNombres(String nombres) {
        this.nombres = nombres;
    }

    /**
     * @return the unidad
     */
    public String getUnidad() {
        return unidad;
    }

    /**
     * @param unidad the unidad to set
     */
    public void setUnidad(String unidad) {
        this.unidad = unidad;
    }

    /**
     * @return the idCategoria
     */
    public int getIdCategoria() {
        return idCategoria;
    }

    /**
     * @param idCategoria the idCategoria to set
     */
    public void setIdCategoria(int idCategoria) {
        this.idCategoria = idCategoria;
    }

    /**
     * @return the imagen
     */
    public String getImagen() {
        return imagen;
    }

    /**
     * @param imagen the imagen to set
     */
    public void setImagen(String imagen) {
        this.imagen = imagen;
    }

    /**
     * @return the catDto
     */
    public CategoriaDto getCatDto() {
        return catDto;
    }

    /**
     * @param catDto the catDto to set
     */
    public void setCatDto(CategoriaDto catDto) {
        this.catDto = catDto;
    }

}
