package modulo.ofertas.dto;

/**
 *
 * @author kennross
 */
public class PromocionDto {

    private int idPromocion = 0;
    private String descripcion = "";
    private float detalle = 0.f;

    @Override
    public String toString() {
        return "PromocionDto{" + "idNovedad=" + idPromocion + ", descripcion=" + descripcion + ", detalle=" + detalle + '}';
    }

    public int getIdPromocion() {
        return idPromocion;
    }

    public void setIdPromocion(int idPromocion) {
        this.idPromocion = idPromocion;
    }

    public String getDescripcion() {
        return descripcion;
    }

    public void setDescripcion(String descripcion) {
        this.descripcion = descripcion;
    }

    public float getDetalle() {
        return detalle;
    }

    public void setDetalle(float detalle) {
        this.detalle = detalle;
    }

}
