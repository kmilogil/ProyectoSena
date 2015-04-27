package modulo.ofertas.dto;

/**
 * @author Kennit David Ruz Romero Hora de creación: 11:18 a.m. Fecha: 19 de
 * Marzo de 2015
 */
public class CategoriaDto {

    private int idCategoria = 0;
    private String descripcion = "";

    @Override
    public String toString() {
        return "CategoriaDto{" + "idCategoria=" + idCategoria + ", descripcion=" + descripcion + '}';
    }

    public int getIdCategoria() {
        return idCategoria;
    }

    public void setIdCategoria(int idCategoria) {
        this.idCategoria = idCategoria;
    }

    public String getDescripcion() {
        return descripcion;
    }

    public void setDescripcion(String descripcion) {
        this.descripcion = descripcion;
    }

}
