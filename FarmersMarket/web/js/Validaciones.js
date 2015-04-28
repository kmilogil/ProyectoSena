var letras = /^[a-zA-Z-ZáéíóúàèìòùÀÈÌÒÙÁÉÍÓÚñÑüÜñÑ ]+$/;

function validarRol(rol) {
    if (rol.selectedIndex === 0) {
        document.getElementById('ruRol').setAttribute('data-toggle', 'tooltip');
        document.getElementById('ruRol').setAttribute('data-original-title', 'Debe selecciona un rol');
        $(document).ready(function() {
            $('[data-toggle="tooltip"]').tooltip({
                placement: 'left'
            });
        });
        document.getElementById('inpRol').setAttribute('class', 'form-group has-error');
        document.getElementById('botonRegistro').setAttribute('disabled', 'true');
    } else {
        document.getElementById('ruRol').removeAttribute('data-toggle', 'tooltip');
        document.getElementById('ruRol').removeAttribute('data-original-title', 'Debe selecciona un rol');
        document.getElementById('inpRol').setAttribute('class', 'form-group has-success');
        document.getElementById('botonRegistro').removeAttribute('disabled');
    }
}
;

function validarDocumento(numeroDocumento) {
    if (numeroDocumento.value === "" || numeroDocumento.value === null) {
        document.getElementById('ruDocumento').setAttribute('data-toggle', 'tooltip');
        document.getElementById('ruDocumento').setAttribute('data-original-title', 'Debe ingresar su número de documento, no debe estar vacio');
        $(document).ready(function() {
            // Initialize tooltip
            $('[data-toggle="tooltip"]').tooltip({
                placement: 'left'
            });
        });
        document.getElementById('inpDocumento').setAttribute('class', 'form-group has-feedback has-error');
        document.getElementById('iconFeedbackDocumento').setAttribute('class', 'glyphicon glyphicon-remove form-control-feedback');
        document.getElementById('botonRegistro').setAttribute('disabled', 'true');
    } else if (numeroDocumento.value < 999999999) {
        document.getElementById('ruDocumento').setAttribute('data-toggle', 'tooltip');
        document.getElementById('ruDocumento').setAttribute('data-original-title', 'Debe ser de 10 digitos el campo');
        $(document).ready(function() {
            // Initialize tooltip
            $('[data-toggle="tooltip"]').tooltip({
                placement: 'left'
            });
        });
        document.getElementById('inpDocumento').setAttribute('class', 'form-group has-feedback has-error');
        document.getElementById('botonRegistro').setAttribute('disabled', 'true');
    } else if (isNaN(numeroDocumento.value)) {
        document.getElementById('ruDocumento').setAttribute('data-toggle', 'tooltip');
        document.getElementById('ruDocumento').setAttribute('data-original-title', 'Debe ingresar sólo números, no se admiten letras');
        $(document).ready(function() {
            // Initialize tooltip
            $('[data-toggle="tooltip"]').tooltip({
                placement: 'left'
            });
        });
        document.getElementById('inpDocumento').setAttribute('class', 'form-group has-feedback has-error');
        document.getElementById('iconFeedbackDocumento').setAttribute('class', 'glyphicon glyphicon-remove form-control-feedback');
        document.getElementById('botonRegistro').setAttribute('disabled', 'true');
    } else {
        document.getElementById('iconFeedbackDocumento').setAttribute('class', 'glyphicon glyphicon-ok form-control-feedback');
        document.getElementById('ruDocumento').removeAttribute('data-toggle', 'tooltip');
        document.getElementById('ruDocumento').removeAttribute('data-original-title', 'Este campo no debe ir vacio');
        document.getElementById('inpDocumento').setAttribute('class', 'form-group has-feedback has-success');
        document.getElementById('iconFeedbackDocumento').setAttribute('class', 'glyphicon glyphicon-ok form-control-feedback');
        document.getElementById('botonRegistro').removeAttribute('disabled');
    }
}
function validarNombres(campoNombres) {
    var letras = /^[a-zA-Z ]+$/;
    if (campoNombres.value === null || campoNombres.value === "") {
        document.getElementById('ruNombres').setAttribute('data-toggle', 'tooltip');
        document.getElementById('ruNombres').setAttribute('data-original-title', 'El campo no puede estar vacio');
        $(document).ready(function() {
            // Initialize tooltip
            $('[data-toggle="tooltip"]').tooltip({
                placement: 'left'
            });
        });
        document.getElementById('inpNombres').setAttribute('class', 'form-group has-feedback has-error');
        document.getElementById('iconFeedbackNombres').setAttribute('class', 'glyphicon glyphicon-remove form-control-feedback');
        document.getElementById('botonRegistro').setAttribute('disabled', 'true');
    } else if (!isNaN(campoNombres.value)) {
        document.getElementById('ruNombres').setAttribute('data-toggle', 'tooltip');
        document.getElementById('ruNombres').setAttribute('data-original-title', 'El nombre no puede ser un número');
        $(document).ready(function() {
            // Initialize tooltip
            $('[data-toggle="tooltip"]').tooltip({
                placement: 'left'
            });
        });
        document.getElementById('inpNombres').setAttribute('class', 'form-group has-feedback has-error');
        document.getElementById('iconFeedbackNombres').setAttribute('class', 'glyphicon glyphicon-remove form-control-feedback');
        document.getElementById('botonRegistro').setAttribute('disabled', 'true');
    } else if (campoNombres.value.length < 3) {
        document.getElementById('ruNombres').setAttribute('data-toggle', 'tooltip');
        document.getElementById('ruNombres').setAttribute('data-original-title', 'Tienen que ser más de 3 carácteres');
        $(document).ready(function() {
            // Initialize tooltip
            $('[data-toggle="tooltip"]').tooltip({
                placement: 'left'
            });
        });
        document.getElementById('inpNombres').setAttribute('class', 'form-group has-feedback has-error');
        document.getElementById('iconFeedbackNombres').setAttribute('class', 'glyphicon glyphicon-remove form-control-feedback');
        document.getElementById('botonRegistro').setAttribute('disabled', 'true');
    } else {
        document.getElementById('ruNombres').removeAttribute('data-toggle', 'tooltip');
        document.getElementById('ruNombres').removeAttribute('data-original-title', 'El nombre no puede ser un número');
        document.getElementById('iconFeedbackNombres').setAttribute('class', 'glyphicon glyphicon-ok form-control-feedback');
        document.getElementById('inpNombres').setAttribute('class', 'form-group has-feedback has-success');
        document.getElementById('iconFeedbackNombres').setAttribute('class', 'glyphicon glyphicon-ok form-control-feedback');
        document.getElementById('botonRegistro').removeAttribute('disabled');
    }
}

function validarApellidos(campoApellidos) {
    if (campoApellidos.value === null || campoApellidos.value === "") {
        document.getElementById('ruApellidos').setAttribute('data-toggle', 'tooltip');
        document.getElementById('ruApellidos').setAttribute('data-original-title', 'El campo no puede estar vacio');
        $(document).ready(function() {
            // Initialize tooltip
            $('[data-toggle="tooltip"]').tooltip({
                placement: 'left'
            });
        });
        document.getElementById('inpApellidos').setAttribute('class', 'form-group has-feedback has-error');
        document.getElementById('iconFeedbackApellidos').setAttribute('class', 'glyphicon glyphicon-remove form-control-feedback');
        document.getElementById('botonRegistro').setAttribute('disabled', 'true');
        document.getElementById('botonRegistro').removeAttribute('disabled');
    } else if (!isNaN(campoApellidos.value)) {
        document.getElementById('ruApellidos').setAttribute('data-toggle', 'tooltip');
        document.getElementById('ruApellidos').setAttribute('data-original-title', 'El apellido no puede ser un número');
        $(document).ready(function() {
            // Initialize tooltip
            $('[data-toggle="tooltip"]').tooltip({
                placement: 'left'
            });
        });
        document.getElementById('inpApellidos').setAttribute('class', 'form-group has-feedback has-error');
        document.getElementById('iconFeedbackNombres').setAttribute('class', 'glyphicon glyphicon-remove form-control-feedback');
        document.getElementById('botonRegistro').setAttribute('disabled', 'true');
    } else if (campoApellidos.value.length < 3) {
        document.getElementById('ruApellidos').setAttribute('data-toggle', 'tooltip');
        document.getElementById('ruApellidos').setAttribute('data-original-title', 'Tienen que ser más de 3 carácteres');
        $(document).ready(function() {
            // Initialize tooltip
            $('[data-toggle="tooltip"]').tooltip({
                placement: 'left'
            });
        });
        document.getElementById('inpApellidos').setAttribute('class', 'form-group has-feedback has-error');
        document.getElementById('iconFeedbackNombres').setAttribute('class', 'glyphicon glyphicon-remove form-control-feedback');
    } else {
        document.getElementById('ruApellidos').removeAttribute('data-toggle', 'tooltip');
        document.getElementById('ruApellidos').removeAttribute('data-original-title', 'El apellido no puede ser un número');
        document.getElementById('inpApellidos').setAttribute('class', 'form-group has-feedback has-success');
        document.getElementById('iconFeedbackApellidos').setAttribute('class', 'glyphicon glyphicon-ok form-control-feedback');
        document.getElementById('botonRegistro').removeAttribute('disabled');
    }
}

function validarCorreo(correo) {
    if (correo.value === null || correo.value === "") {
        document.getElementById('ruCorreo').setAttribute('data-toggle', 'tooltip');
        document.getElementById('ruCorreo').setAttribute('data-original-title', 'El campo no puede estar vacio');
        $(document).ready(function() {
            // Initialize tooltip
            $('[data-toggle="tooltip"]').tooltip({
                placement: 'left'
            });
        });
        document.getElementById('inpCorreo').setAttribute('class', 'form-group has-feedback has-error');
        document.getElementById('iconFeedbackCorreo').setAttribute('class', 'glyphicon glyphicon-remove form-control-feedback');
        document.getElementById('botonRegistro').setAttribute('disabled', 'true');
    } else if (!(/^([a-zA-Z0-9_\.\-])+\@(([a-zA-Z0-9\-])+\.)+([a-zA-Z0-9]{2,4})+$/.test(correo.value))) {
        document.getElementById('ruCorreo').setAttribute('data-toggle', 'tooltip');
        document.getElementById('ruCorreo').setAttribute('data-original-title', 'El correo no está en un formato correcto, correo@ejemplo.com');
        $(document).ready(function() {
            // Initialize tooltip
            $('[data-toggle="tooltip"]').tooltip({
                placement: 'left'
            });
        });
        document.getElementById('inpCorreo').setAttribute('class', 'form-group has-feedback has-error');
        document.getElementById('iconFeedbackCorreo').setAttribute('class', 'glyphicon glyphicon-remove form-control-feedback');
        document.getElementById('ruCorreo2').setAttribute('disabled');
        document.getElementById('botonRegistro').setAttribute('disabled', 'true');
    } else {
        document.getElementById('ruCorreo').removeAttribute('data-toggle', 'tooltip');
        document.getElementById('ruCorreo2').removeAttribute('disabled');
        document.getElementById('ruCorreo').removeAttribute('data-original-title', 'El correo no está en un formato correcto, correo@ejemplo.com');
        document.getElementById('inpCorreo').setAttribute('class', 'form-group has-feedback has-success');
        document.getElementById('iconFeedbackCorreo').setAttribute('class', 'glyphicon glyphicon-ok form-control-feedback');
        document.getElementById('botonRegistro').removeAttribute('disabled');
    }
}

function validarCorreo2(correo) {
    if (correo.value === null || correo.value === "") {
        document.getElementById('rpEmail').setAttribute('data-toggle', 'tooltip');
        document.getElementById('rpEmail').setAttribute('data-original-title', 'El campo no puede estar vacio');
        $(document).ready(function() {
            // Initialize tooltip
            $('[data-toggle="tooltip"]').tooltip({
                placement: 'left'
            });
        });
        document.getElementById('inpCorreo2').setAttribute('class', 'form-group has-feedback has-error');
        document.getElementById('iconFeedbackCorreo2').setAttribute('class', 'glyphicon glyphicon-remove form-control-feedback');
        document.getElementById('btnRecuperar').setAttribute('disabled', 'true');
    } else if (!(/\w+([-+.']\w+)*@\w+([-.]\w+)*\.\w+([-.]\w+)/.test(correo.value))) {
        document.getElementById('rpEmail').setAttribute('data-toggle', 'tooltip');
        document.getElementById('rpEmail').setAttribute('data-original-title', 'El correo no está en un formato correcto, correo@ejemplo.com');
        $(document).ready(function() {
            // Initialize tooltip
            $('[data-toggle="tooltip"]').tooltip({
                placement: 'left'
            });
        });
        document.getElementById('inpCorreo2').setAttribute('class', 'form-group has-feedback has-error');
        document.getElementById('iconFeedbackCorreo2').setAttribute('class', 'glyphicon glyphicon-remove form-control-feedback');
        document.getElementById('btnRecuperar').setAttribute('disabled', 'true');
    } else {
        document.getElementById('rpEmail').removeAttribute('data-toggle', 'tooltip');
        document.getElementById('rpEmail').removeAttribute('data-original-title', 'El correo no está en un formato correcto, correo@ejemplo.com');
        document.getElementById('inpCorreo2').setAttribute('class', 'form-group has-feedback has-success');
        document.getElementById('iconFeedbackCorreo2').setAttribute('class', 'glyphicon glyphicon-ok form-control-feedback');
        document.getElementById('btnRecuperar').removeAttribute('disabled');
    }
}

function validarRepetirCorreo(correo1) {
    var correo2 = document.getElementById('ruCorreo');
    if (correo1.value === null || correo1.value === "") {
        document.getElementById('ruCorreo2').setAttribute('data-toggle', 'tooltip');
        document.getElementById('ruCorreo2').setAttribute('data-original-title', 'El campo no puede estar vacio');
        $(document).ready(function() {
            // Initialize tooltip
            $('[data-toggle="tooltip"]').tooltip({
                placement: 'left'
            });
        });
        document.getElementById('inpCorreoRepetido').setAttribute('class', 'form-group has-feedback has-error');
        document.getElementById('iconFeedbackCorreo2').setAttribute('class', 'glyphicon glyphicon-remove form-control-feedback');
        document.getElementById('botonRegistro').setAttribute('disabled', 'true');
    } else if (correo1.value !== correo2.value) {
        document.getElementById('ruCorreo2').setAttribute('data-toggle', 'tooltip');
        document.getElementById('ruCorreo2').setAttribute('data-original-title', 'Los correos no coinciden, verifique');
        $(document).ready(function() {
            // Initialize tooltip
            $('[data-toggle="tooltip"]').tooltip({
                placement: 'left'
            });
        });
        document.getElementById('inpCorreoRepetido').setAttribute('class', 'form-group has-feedback has-error');
        document.getElementById('iconFeedbackCorreo2').setAttribute('class', 'glyphicon glyphicon-remove form-control-feedback');
        document.getElementById('botonRegistro').setAttribute('disabled', 'true');
    } else {
        document.getElementById('ruCorreo2').removeAttribute('data-toggle', 'tooltip');
        document.getElementById('ruCorreo2').removeAttribute('data-original-title');
        document.getElementById('inpCorreoRepetido').setAttribute('class', 'form-group has-feedback has-success');
        document.getElementById('iconFeedbackCorreo2').setAttribute('class', 'glyphicon glyphicon-ok form-control-feedback');
        document.getElementById('botonRegistro').removeAttribute('disabled');
    }
}

function validarDireccion(direccion) {
    if (direccion.value === null || direccion.value === "") {
        document.getElementById('ruDireccion').setAttribute('data-toggle', 'tooltip');
        document.getElementById('ruDireccion').setAttribute('data-original-title', 'El campo no puede estar vacio');
        $(document).ready(function() {
            // Initialize tooltip
            $('[data-toggle="tooltip"]').tooltip({
                placement: 'top'
            });
        });
        document.getElementById('inpDireccion').setAttribute('class', 'form-group has-feedback has-error');
        document.getElementById('iconFeedbackDireccion').setAttribute('class', 'glyphicon glyphicon-remove form-control-feedback');
        document.getElementById('botonRegistro').setAttribute('disabled', 'true');
    } else {
        document.getElementById('ruDireccion').removeAttribute('data-toggle', 'tooltip');
        document.getElementById('ruDireccion').removeAttribute('data-original-title');
        document.getElementById('inpDireccion').setAttribute('class', 'form-group has-feedback has-success');
        document.getElementById('botonRegistro').removeAttribute('disabled');
    }
}


function validarDepartamento(departamento) {
    if (departamento.value === null || departamento.value === "") {
        document.getElementById('ruDepartamento').setAttribute('data-toggle', 'tooltip');
        document.getElementById('ruDepartamento').setAttribute('data-original-title', 'Debe seleccionar un departamento');
        $(document).ready(function() {
            // Initialize tooltip
            $('[data-toggle="tooltip"]').tooltip({
                placement: 'top'
            });
        });
        document.getElementById('inpDepartamento').setAttribute('class', 'form-group has-feedback has-error');
        document.getElementById('botonRegistro').setAttribute('disabled', 'true');
    } else {
        document.getElementById('ruDepartamento').removeAttribute('data-toggle', 'tooltip');
        document.getElementById('ruDepartamento').removeAttribute('data-original-title');
        document.getElementById('inpDepartamento').setAttribute('class', 'form-group has-feedback has-success');
        document.getElementById('botonRegistro').removeAttribute('disabled');
    }
}

function validarCiudad(ciudad) {
    if (ciudad.value === null || ciudad.value === "") {
        document.getElementById('ruCiudad').setAttribute('data-toggle', 'tooltip');
        document.getElementById('ruCiudad').setAttribute('data-original-title', 'Debe seleccionar un departamento antes');
        $(document).ready(function() {
            // Initialize tooltip
            $('[data-toggle="tooltip"]').tooltip({
                placement: 'top'
            });
        });
        document.getElementById('inpCiudad').setAttribute('class', 'form-group has-feedback has-error');
        document.getElementById('botonRegistro').setAttribute('disabled', 'true');
    } else {
        document.getElementById('ruCiudad').removeAttribute('data-toggle', 'tooltip');
        document.getElementById('ruCiudad').removeAttribute('data-original-title');
        document.getElementById('inpCiudad').setAttribute('class', 'form-group has-feedback has-success');
        document.getElementById('botonRegistro').removeAttribute('disabled');
    }
}

function validarClave(clave) {
    if (clave.value === null || clave.value === "") {
        document.getElementById('ruClave').setAttribute('data-toggle', 'tooltip');
        document.getElementById('ruClave').setAttribute('data-original-title', 'Debe ingresar una contraseña');
        $(document).ready(function() {
            // Initialize tooltip
            $('[data-toggle="tooltip"]').tooltip({
                placement: 'top'
            });
        });
        document.getElementById('inpClave').setAttribute('class', 'form-group has-feedback has-error');
        document.getElementById('iconFeedbackClave').setAttribute('class', 'glyphicon glyphicon-remove form-control-feedback');
        document.getElementById('botonRegistro').setAttribute('disabled', 'true');
    } else if (clave.value.length < 6) {
        document.getElementById('ruClave').setAttribute('data-toggle', 'tooltip');
        document.getElementById('ruClave').setAttribute('data-original-title', 'Tiene que ser de más de 6 caráctres');
        $(document).ready(function() {
            // Initialize tooltip
            $('[data-toggle="tooltip"]').tooltip({
                placement: 'left'
            });
        });
        document.getElementById('inpClave').setAttribute('class', 'form-group has-feedback has-error');
        document.getElementById('iconFeedbackClave').setAttribute('class', 'glyphicon glyphicon-remove form-control-feedback');
        document.getElementById('botonRegistro').setAttribute('disabled', 'true');
    } else {
        document.getElementById('ruClave').removeAttribute('data-toggle', 'tooltip');
        document.getElementById('ruClave').removeAttribute('data-original-title');
        document.getElementById('inpClave').setAttribute('class', 'form-group has-feedback has-success');
        document.getElementById('iconFeedbackClave').setAttribute('class', 'glyphicon glyphicon-ok form-control-feedback');
        document.getElementById('botonRegistro').removeAttribute('disabled');
    }
}

function validarCaptcha(elemento) {
    if (elemento.value === null || elemento.value === "") {
        document.getElementById('answer').setAttribute('data-toggle', 'tooltip');
        document.getElementById('answer').setAttribute('data-original-title', 'captcha');
        $(document).ready(function() {
            // Initialize tooltip
            $('[data-toggle="tooltip"]').tooltip({
                placement: 'top'
            });
        });
        document.getElementById('inpCaptcha').setAttribute('class', 'form-group has-feedback has-error');
        document.getElementById('botonRegistro').setAttribute('disabled', 'true');
    } else {
        document.getElementById('answer').removeAttribute('data-toggle', 'tooltip');
        document.getElementById('answer').removeAttribute('data-original-title');
        document.getElementById('inpCaptcha').setAttribute('class', 'form-group has-feedback has-success');
        document.getElementById('botonRegistro').removeAttribute('disabled');
    }
}

function validarFecha(fecha) {
    if (fecha.value === null || fecha.value === "") {
        document.getElementById('ruFecha').setAttribute('data-toggle', 'tooltip');
        document.getElementById('ruFecha').setAttribute('data-original-title', 'Debe ingresar su fecha de nacimiento');
        $(document).ready(function() {
            // Initialize tooltip
            $('[data-toggle="tooltip"]').tooltip({
                placement: 'top'
            });
        });
        document.getElementById('inpFecha').setAttribute('class', 'form-group has-feedback has-error');
        document.getElementById('iconFeedbackFecha').setAttribute('class', 'glyphicon glyphicon-remove form-control-feedback');
        document.getElementById('botonRegistro').setAttribute('disabled', 'true');
    } else {
        document.getElementById('ruFecha').removeAttribute('data-toggle', 'tooltip');
        document.getElementById('ruFecha').removeAttribute('data-original-title');
        document.getElementById('inpFecha').setAttribute('class', 'form-group has-feedback has-success');
        document.getElementById('iconFeedbackFecha').setAttribute('class', 'glyphicon glyphicon-ok form-control-feedback');
        document.getElementById('botonRegistro').removeAttribute('disabled');
    }
}

function enviarFormulario(nombreForm) {
    document.getElementById(nombreForm).submit();
}


function validarClaveEnCambiar(clave) {
    if (clave.value === null || clave.value === "") {
        document.getElementById('ccClaveAntigua').setAttribute('data-toggle', 'tooltip');
        document.getElementById('ccClaveAntigua').setAttribute('data-original-title', 'El campo no puede estar vacio');
        $(document).ready(function() {
            // Initialize tooltip
            $('[data-toggle="tooltip"]').tooltip({
                placement: 'top'
            });
        });
        document.getElementById('inpClaveAntigua').setAttribute('class', 'form-group has-feedback has-error');
        document.getElementById('iconFeedbackClaveCambiar').setAttribute('class', 'glyphicon glyphicon-remove form-control-feedback');
        document.getElementById('botonEnviarCambiarClave').setAttribute('disabled', 'true');
    } else if (clave.value.length < 6) {
        document.getElementById('ccClaveAntigua').setAttribute('data-toggle', 'tooltip');
        document.getElementById('ccClaveAntigua').setAttribute('data-original-title', 'Tiene que ser de más de 6 caráctres');
        $(document).ready(function() {
            // Initialize tooltip
            $('[data-toggle="tooltip"]').tooltip({
                placement: 'left'
            });
        });
        document.getElementById('inpClaveAntigua').setAttribute('class', 'form-group has-feedback has-error');
        document.getElementById('iconFeedbackClaveCambiar').setAttribute('class', 'glyphicon glyphicon-remove form-control-feedback');
        document.getElementById('botonEnviarCambiarClave').setAttribute('disabled', 'true');
    } else {
        document.getElementById('ccClaveAntigua').removeAttribute('data-toggle', 'tooltip');
        document.getElementById('ccClaveAntigua').removeAttribute('data-original-title');
        document.getElementById('inpClaveAntigua').setAttribute('class', 'form-group has-feedback has-success');
        document.getElementById('iconFeedbackClaveCambiar').setAttribute('class', 'glyphicon glyphicon-ok form-control-feedback');
        document.getElementById('botonEnviarCambiarClave').removeAttribute('disabled');
    }
}

function validarClaveNuevaEnCambiar(clave) {
    if (clave.value === null || clave.value === "") {
        document.getElementById('ccClaveNueva').setAttribute('data-toggle', 'tooltip');
        document.getElementById('ccClaveNueva').setAttribute('data-original-title', 'El campo no puede estar vacio');
        $(document).ready(function() {
            // Initialize tooltip
            $('[data-toggle="tooltip"]').tooltip({
                placement: 'top'
            });
        });
        document.getElementById('inpClaveNuevaCambiar').setAttribute('class', 'form-group has-feedback has-error');
        document.getElementById('iconFeedbackClaveNuevaCambiar').setAttribute('class', 'glyphicon glyphicon-remove form-control-feedback');
        document.getElementById('botonEnviarCambiarClave').setAttribute('disabled', 'true');
    } else if (clave.value.length < 6) {
        document.getElementById('ccClaveNueva').setAttribute('data-toggle', 'tooltip');
        document.getElementById('ccClaveNueva').setAttribute('data-original-title', 'Tiene que ser de más de 6 caráctres');
        $(document).ready(function() {
            // Initialize tooltip
            $('[data-toggle="tooltip"]').tooltip({
                placement: 'left'
            });
        });
        document.getElementById('inpClaveNuevaCambiar').setAttribute('class', 'form-group has-feedback has-error');
        document.getElementById('iconFeedbackClaveNuevaCambiar').setAttribute('class', 'glyphicon glyphicon-remove form-control-feedback');
        document.getElementById('botonEnviarCambiarClave').setAttribute('disabled', 'true');
    } else {
        document.getElementById('ccClaveNueva').removeAttribute('data-toggle', 'tooltip');
        document.getElementById('ccClaveNueva').removeAttribute('data-original-title');
        document.getElementById('inpClaveNuevaCambiar').setAttribute('class', 'form-group has-feedback has-success');
        document.getElementById('iconFeedbackClaveNuevaCambiar').setAttribute('class', 'glyphicon glyphicon-ok form-control-feedback');
        document.getElementById('botonEnviarCambiarClave').removeAttribute('disabled');
    }
}

function validarRepetirClaveNuevaEnCambiar(correo1) {
    var correo2 = document.getElementById('ccClaveNueva');
    if (correo1.value === null || correo1.value === "") {
        document.getElementById('ccClaveRepetida').setAttribute('data-toggle', 'tooltip');
        document.getElementById('ccClaveRepetida').setAttribute('data-original-title', 'El campo no puede estar vacio');
        $(document).ready(function() {
            // Initialize tooltip
            $('[data-toggle="tooltip"]').tooltip({
                placement: 'left'
            });
        });
        document.getElementById('inpClaveRepetidaCambiar').setAttribute('class', 'form-group has-feedback has-error');
        document.getElementById('iconFeedbackClaveNuevaCambiar2').setAttribute('class', 'glyphicon glyphicon-remove form-control-feedback');
        document.getElementById('botonEnviarCambiarClave').setAttribute('disabled', 'true');
    } else if (correo1.value !== correo2.value) {
        document.getElementById('ccClaveRepetida').setAttribute('data-toggle', 'tooltip');
        document.getElementById('ccClaveRepetida').setAttribute('data-original-title', 'Los contraseñas no coinciden, verifique');
        $(document).ready(function() {
            // Initialize tooltip
            $('[data-toggle="tooltip"]').tooltip({
                placement: 'left'
            });
        });
        document.getElementById('inpClaveRepetidaCambiar').setAttribute('class', 'form-group has-feedback has-error');
        document.getElementById('iconFeedbackClaveNuevaCambiar2').setAttribute('class', 'glyphicon glyphicon-remove form-control-feedback');
        document.getElementById('botonEnviarCambiarClave').setAttribute('disabled', 'true');
    } else {
        document.getElementById('ccClaveRepetida').removeAttribute('data-toggle', 'tooltip');
        document.getElementById('ccClaveRepetida').removeAttribute('data-original-title');
        document.getElementById('inpClaveRepetidaCambiar').setAttribute('class', 'form-group has-feedback has-success');
        document.getElementById('iconFeedbackClaveNuevaCambiar2').setAttribute('class', 'glyphicon glyphicon-ok form-control-feedback');
        document.getElementById('botonEnviarCambiarClave').removeAttribute('disabled');
    }

    function enviarFormulario(miForm) {
        document.getElementById(miForm);
    }
}

