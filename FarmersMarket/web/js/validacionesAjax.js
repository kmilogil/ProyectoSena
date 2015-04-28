var req1;
var req2;
var req3;
var xmlHttp;
//Función para verificar si el documento ya está registrado anteriormente
function validarUsuarioYaRegistrado() {
    var campo = document.getElementById("ruDocumento");
    if (campo.value === '')
        return false;

    var url = "PeticionesAjax?idUsuario=" + escape(campo.value);
    if (window.XMLHttpRequest) {
        req1 = new XMLHttpRequest();
    }
    else if (window.ActiveXObject) {
        req1 = new ActiveXObject("Microsoft.XMLHTTP");
    }
    req1.open("Get", url, true);
    req1.onreadystatechange = callbackIdUsuario;
    req1.send(null);
}
//Acciones que se desatan en pos de la verificación del documento
function callbackIdUsuario() {
    if (req1.readyState === 4) {
        if (req1.status === 200) {
            if (req1.responseText.toString() === "existe") {
                document.getElementById('ruDocumento').setAttribute('data-toggle', 'tooltip');
                document.getElementById('ruDocumento').setAttribute('data-original-title', 'Usuario ya registrado anteriormente, intente iniciar sesión');
                $(document).ready(function() {
                    // Initialize tooltip
                    $('[data-toggle="tooltip"]').tooltip({
                        placement: 'left'
                    });
                });
                document.getElementById('inpDocumento').setAttribute('class', 'form-group has-feedback has-error');
                document.getElementById('iconFeedbackDocumento').setAttribute('class', 'glyphicon glyphicon-remove form-control-feedback');
                document.getElementById('botonRegistro').setAttribute('disabled', 'true');
                document.getElementById('ruNombres').setAttribute('disabled', 'true');
                document.getElementById('ruApellidos').setAttribute('disabled', 'true');
                document.getElementById('ruClave').setAttribute('disabled', 'true');
                document.getElementById('ruFecha').setAttribute('disabled', 'true');
                document.getElementById('ruDepartamento').setAttribute('disabled', 'true');
                document.getElementById('ruCiudad').setAttribute('disabled', 'true');
                document.getElementById('ruRol').setAttribute('disabled', 'true');
                document.getElementById('ruCorreo').setAttribute('disabled', 'true');
                document.getElementById('ruCorreo2').setAttribute('disabled', 'true');
                document.getElementById('ruDireccion').setAttribute('disabled', 'true');
                document.getElementById('ruTerminos').setAttribute('disabled', 'true');
            } else if (req1.responseText.toString() === "noexiste") {
                document.getElementById('ruDocumento').removeAttribute('data-toggle', 'tooltip');
                document.getElementById('ruDocumento').removeAttribute('data-original-title');
                document.getElementById('inpDocumento').setAttribute('class', 'form-group has-feedback has-success');
                document.getElementById('iconFeedbackDocumento').setAttribute('class', 'glyphicon glyphicon-ok form-control-feedback');
                document.getElementById('botonRegistro').removeAttribute('disabled');

                document.getElementById('ruNombres').removeAttribute('disabled');
                document.getElementById('ruApellidos').removeAttribute('disabled');
                document.getElementById('ruClave').removeAttribute('disabled');
                document.getElementById('ruFecha').removeAttribute('disabled');
                document.getElementById('ruDepartamento').removeAttribute('disabled');
                document.getElementById('ruCiudad').removeAttribute('disabled');
                document.getElementById('ruRol').removeAttribute('disabled');
                document.getElementById('ruCorreo').removeAttribute('disabled');
                document.getElementById('ruCorreo2').removeAttribute('disabled');
                document.getElementById('ruDireccion').removeAttribute('disabled');
                document.getElementById('ruTerminos').removeAttribute('disabled');
            }
        }
    }
}



//Función para verificar si el coreo ya está registrado anteriormente
function validaCorreoYaRegistrado(campo) {
    if (campo.value === '')
        return false;

    var url = "PeticionesAjax?correo=" + escape(campo.value);
    if (window.XMLHttpRequest) {
        req2 = new XMLHttpRequest();
    }
    else if (window.ActiveXObject) {
        req2 = new ActiveXObject("Microsoft.XMLHTTP");
    }
    req2.open("Get", url, true);
    req2.onreadystatechange = callbackCorreo;
    req2.send(null);
}
//Acciones que se desatan en pos de la verificación del documento
function callbackCorreo() {
    if (req2.readyState === 4) {
        if (req2.status === 200) {
            if (req2.responseText.toString() === "existe") {
                document.getElementById('ruCorreo').setAttribute('data-toggle', 'tooltip');
                document.getElementById('ruCorreo').setAttribute('data-original-title', 'Correo ya registrado anteriormente');
                $(document).ready(function() {
                    // Initialize tooltip
                    $('[data-toggle="tooltip"]').tooltip({
                        placement: 'left'
                    });
                });
                document.getElementById('inpCorreo').setAttribute('class', 'form-group has-feedback has-error');
                document.getElementById('iconFeedbackCorreo').setAttribute('class', 'glyphicon glyphicon-remove form-control-feedback');
                document.getElementById('ruCorreo2').setAttribute('disabled', 'true');
                document.getElementById('botonRegistro').setAttribute('disabled', 'true');
                document.getElementById('ruDocumento').setAttribute('disabled', 'true');
                document.getElementById('ruNombres').setAttribute('disabled', 'true');
                document.getElementById('ruApellidos').setAttribute('disabled', 'true');
                document.getElementById('ruClave').setAttribute('disabled', 'true');
                document.getElementById('ruFecha').setAttribute('disabled', 'true');
                document.getElementById('ruDepartamento').setAttribute('disabled', 'true');
                document.getElementById('ruCiudad').setAttribute('disabled', 'true');
                document.getElementById('ruRol').setAttribute('disabled', 'true');
                document.getElementById('ruDireccion').setAttribute('disabled', 'true');
                document.getElementById('ruTerminos').setAttribute('disabled', 'true');
                document.getElementById('ruCorreo2').setAttribute('data-toggle', 'tooltip');
                document.getElementById('ruCorreo2').setAttribute('data-original-title', 'Se deshabilito el campo porque el correo anterior está registrado ya');
            } else if (req2.responseText.toString() === "noexiste") {
                document.getElementById('ruCorreo').removeAttribute('data-toggle', 'tooltip');
                document.getElementById('ruCorreo').removeAttribute('data-original-title');

                document.getElementById('ruCorreo2').removeAttribute('data-toggle', 'tooltip');
                document.getElementById('ruCorreo2').removeAttribute('data-original-title');


                document.getElementById('iconFeedbackCorreo').setAttribute('class', 'glyphicon glyphicon-ok form-control-feedback');
                document.getElementById('inpCorreo').setAttribute('class', 'form-group has-feedback has-success');
                document.getElementById('botonRegistro').removeAttribute('disabled');
                document.getElementById('ruNombres').removeAttribute('disabled');
                document.getElementById('ruApellidos').removeAttribute('disabled');
                document.getElementById('ruClave').removeAttribute('disabled');
                document.getElementById('ruFecha').removeAttribute('disabled');
                document.getElementById('ruDepartamento').removeAttribute('disabled');
                document.getElementById('ruCiudad').removeAttribute('disabled');
                document.getElementById('ruRol').removeAttribute('disabled');
                document.getElementById('ruCorreo2').removeAttribute('disabled');
                document.getElementById('ruDireccion').removeAttribute('disabled');
                document.getElementById('ruDocumento').removeAttribute('disabled');
                document.getElementById('ruTerminos').removeAttribute('disabled');
            }
        }
    }
}

////Función para actualizar la cantidad disponible de una oferta
//function actualizarCantidad() {
//    var campo = document.getElementById("");
//    if (campo.value === '')
//        return false;
//
//    var url = "PeticionesAjax?idOferta=" + escape(campo.value);
//    if (window.XMLHttpRequest) {
//        req3 = new XMLHttpRequest();
//    }
//    else if (window.ActiveXObject) {
//        req3 = new ActiveXObject("Microsoft.XMLHTTP");
//    }
//    req3.open("Get", url, true);
//    req3.onreadystatechange = callbackCantidad;
//    req3.send(null);
//}
////Acciones que se desatan en pos de la verificación de la cantidad
//function callbackCantidad() {
//    if (req3.readyState === 4) {
//        if (req3.status === 200) {
//            if (req3.responseText.toString() !== 0) {
//                document.getElementById().innerHTML === xmlHttp.responseText.toString();
//            }
//
//        }
//    }
//}



var intervalo = 0;
function tiempoCantidad() {
    intervalo = setInterval('getCantidad()', 800);
}

function quitarIntervalo() {
    clearInterval(intervalo);
}

// Realizar Pedido
var xmlHttp;
function getCantidad() {
    var campo = document.getElementById("idOferta");

    if (window.XMLHttpRequest) {
        xmlHttp = new XMLHttpRequest();
    }
    else if (window.ActiveXObject) {
        xmlHttp = new ActiveXObject("Microsoft.XMLHTTP");
    } else {
        alert("El navegador no soporta Ajax!");
        return;
    }

    var url = "../PeticionesAjax?idOferta=" + escape(campo.value);
    xmlHttp.onreadystatechange = resultadoCantidad;
    xmlHttp.open("GET", url, true);
    xmlHttp.send(null);

}

function resultadoCantidad() {
    if (xmlHttp.readyState === 4) {
        document.getElementById("cantidad").innerHTML = xmlHttp.responseText;
    }
}


var xmlHttp;
function getOfertaNovedad(idOferta) {

    if (window.XMLHttpRequest) {
        xmlHttp = new XMLHttpRequest();
    }
    else if (window.ActiveXObject) {
        xmlHttp = new ActiveXObject("Microsoft.XMLHTTP");
    } else {
        alert("El navegador no soporta Ajax!");
        return;
    }

    var url = "../PeticionesAjax?idOfertaP=" + idOferta;
    xmlHttp.onreadystatechange = resultadoPromociones;
    xmlHttp.open("GET", url, true);
    xmlHttp.send(null);

}

function resultadoPromociones() {
    if (xmlHttp.readyState === 4) {
        document.getElementById("formularioPromociones").innerHTML = xmlHttp.responseText;
    }
}


function tiempoCalculo() {
    setInterval('getCalculo()', 800);
}
var xmlHttp;
function getCalculo() {
    var cantidad = document.getElementById("cantidadPedir");
    var idOferta = document.getElementById("idOferta");
    if (window.XMLHttpRequest) {
        xmlHttp = new XMLHttpRequest();
    }
    else if (window.ActiveXObject) {
        xmlHttp = new ActiveXObject("Microsoft.XMLHTTP");
    } else {
        alert("El navegador no soporta Ajax!");
        return;
    }

    var url = "../PeticionesAjax?idOfertaC=" + idOferta.value + "&cantidad=" + cantidad.value;
    xmlHttp.onreadystatechange = resultadoCalculo;
    xmlHttp.open("GET", url, true);
    xmlHttp.send(null);

}

function resultadoCalculo() {
    if (xmlHttp.readyState === 4) {
        if (xmlHttp.responseText !== null) {
            document.getElementById("total").innerHTML = xmlHttp.responseText;
        }

    }
}

function limpiar() {
    document.getElementById("fomularioRealizarPedido").reset();
}

function limpiarIndex() {
    document.getElementById("ingresarSistema").reset();
}

var xmlHttp;
function actualizarCantidad(cantidad, idOferta, idPedido) {

    if (window.XMLHttpRequest) {
        xmlHttp = new XMLHttpRequest();
    }
    else if (window.ActiveXObject) {
        xmlHttp = new ActiveXObject("Microsoft.XMLHTTP");
    } else {
        alert("El navegador no soporta Ajax!");
        return;
    }

    var url = "../PeticionesAjax?cantidadPedida=" + cantidad.value + "&idOfer=" + idOferta + "&idPedi=" + idPedido;
    xmlHttp.onreadystatechange = resultadoCarrito;
    xmlHttp.open("GET", url, true);
    xmlHttp.send(null);

}
function resultadoCarrito() {
    if (xmlHttp.readyState === 4) {
        if (xmlHttp.responseText !== null) {
            document.getElementById("TotalPago").innerHTML = xmlHttp.responseText;
        }
    }
}






///*
// * Parametros mandatorios
// */
//var seconds = 2; // el tiempo en que se refresca
//var divid = "cantidadDisponible"; // el div que quieres actualizar!
//var url = "../PeticionesAjax"; // el archivo que ira en el div
//
//function refreshdiv(idOferta) {
//
//    // The XMLHttpRequest object
//    
//    var xmlHttp;
//    try {
//        xmlHttp = new XMLHttpRequest(); // Firefox, Opera 8.0+, Safari
//    }
//    catch (e) {
//        try {
//            xmlHttp = new ActiveXObject("Msxml2.XMLHTTP"); // Internet Explorer
//        }
//        catch (e) {
//            try {
//                xmlHttp = new ActiveXObject("Microsoft.XMLHTTP");
//            }
//            catch (e) {
//                alert("Tu explorador no soporta AJAX.");
//                return false;
//            }
//        }
//    }
//
//    // Timestamp for preventing IE caching the GET request
//    
//    var nocacheurl = url + "?idOferta=" + idOferta;
//
//    // The code...
//
//    xmlHttp.onreadystatechange = function() {
//        if (xmlHttp.readyState === 4 && xmlHttp.readyState !== null) {
//            document.getElementById(divid).innerHTML = xmlHttp.responseText;
//            setTimeout('refreshdiv()', seconds * 1000);
//        }
//    };
//    xmlHttp.open("GET", nocacheurl, true);
//    xmlHttp.send(null);
//}
//
//// Empieza la función de refrescar
//
//window.onload = function() {
//    refreshdiv(); // corremos inmediatamente la funcion
//};



//function tiempoDisponible(idOferta) {
//    setInterval('getCantidadDisponible(idOferta)', 800);
//}
//
//var xmlHttp;
//function getCantidadDisponible(idOferta) {
//
//    if (window.XMLHttpRequest) {
//        xmlHttp = new XMLHttpRequest();
//    }
//    else if (window.ActiveXObject) {
//        xmlHttp = new ActiveXObject("Microsoft.XMLHTTP");
//    } else {
//        alert("El navegador no soporta Ajax!");
//        return;
//    }
//
//    var url = "../PeticionesAjax?idOferta=" + idOferta;
//    xmlHttp.onreadystatechange = resultadoCantidadDisponible;
//    xmlHttp.open("GET", url, true);
//    xmlHttp.send(null);
//
//}
//
//
//function resultadoCantidadDisponible() {
//    if (xmlHttp.readyState === 4) {
//        document.getElementById("cantidadDisponible").innerHTML = xmlHttp.responseText;
//    }
//}

